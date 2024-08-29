package org.springframework.web.servlet.mvc.method.annotation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.moodminds.emission.Emittable;
import org.moodminds.traverse.Traversable;
import org.springframework.core.MethodParameter;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.server.DelegatingServerHttpResponse;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpResponse;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.web.HttpMediaTypeNotAcceptableException;
import org.springframework.web.accept.ContentNegotiationManager;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.context.request.async.DeferredResult;
import org.springframework.web.method.support.HandlerMethodReturnValueHandler;
import org.springframework.web.method.support.ModelAndViewContainer;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter.Handler;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter.SseEventBuilder;
import org.moodminds.traverse.TraverseSupport;
import org.moodminds.valuable.Volatile.Boolean;

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static org.moodminds.traverse.TakeTraversable.take;
import static org.moodminds.traverse.Traversable.each;
import static org.springframework.core.ResolvableType.forMethodParameter;
import static org.springframework.http.MediaType.*;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;
import static org.springframework.web.context.request.async.WebAsyncUtils.getAsyncManager;
import static org.springframework.web.filter.ShallowEtagHeaderFilter.disableContentCaching;
import static org.springframework.web.servlet.HandlerMapping.PRODUCIBLE_MEDIA_TYPES_ATTRIBUTE;
import static org.moodminds.function.Testable.not;
import static org.moodminds.valuable.Volatile.vol;

/**
 * Handler for return values of types {@link Emittable} and {@link Traversable} and subclasses
 * including the same types wrapped with {@link ResponseEntity}. Performs async handling using
 * the {@link TaskExecutor} provided.
 */
public class ResponseBodyTraverseSupportReturnValueHandler implements HandlerMethodReturnValueHandler {

    private static final Log logger = LogFactory.getLog(ResponseBodyTraverseSupportReturnValueHandler.class);

    private static final long STREAMING_TIMEOUT = -1;

    @SuppressWarnings("deprecation")
    private static final List<MediaType> JSON_STREAMING_MEDIA_TYPES = asList(APPLICATION_NDJSON, APPLICATION_STREAM_JSON);


    private final TaskExecutor taskExecutor;
    private final List<HttpMessageConverter<?>> converters;
    private final ContentNegotiationManager contentNegotiationManager;

    private final AtomicBoolean warnTaskExecutor;

    /**
     * Construct the object with the given arguments.
     *
     * @param taskExecutor the given {@link TaskExecutor}
     * @param messageConverters the given list of {@link HttpMessageConverter}
     * @param contentNegotiationManager the given {@link ContentNegotiationManager}
     */
    public ResponseBodyTraverseSupportReturnValueHandler(TaskExecutor taskExecutor,
                                                         List<HttpMessageConverter<?>> messageConverters,
                                                         ContentNegotiationManager contentNegotiationManager) {

        Assert.notNull(taskExecutor, "TaskExecutor is required");
        Assert.notNull(messageConverters, "Message Converters are required");
        Assert.notNull(contentNegotiationManager, "ContentNegotiationManager is required");

        this.taskExecutor = requireNonNull(taskExecutor, "ContentNegotiationManager is required");
        this.converters = ensureTextPlainWrite(requireNonNull(messageConverters));
        this.contentNegotiationManager = requireNonNull(contentNegotiationManager);

        this.warnTaskExecutor = new AtomicBoolean((taskExecutor instanceof SimpleAsyncTaskExecutor
                || taskExecutor instanceof SyncTaskExecutor));
    }

    @Override
    public boolean supportsReturnType(MethodParameter returnType) {
        Class<?> bodyType = ResponseEntity.class.isAssignableFrom(returnType.getParameterType()) ?
                forMethodParameter(returnType).getGeneric().resolve() :
                returnType.getParameterType();
        return bodyType != null && (Emittable.class.isAssignableFrom(bodyType)
                || Traversable.class.isAssignableFrom(bodyType));
    }

    @Override
    public void handleReturnValue(Object returnValue, MethodParameter returnType, ModelAndViewContainer mavContainer,
                                  NativeWebRequest request) throws Exception {

        if (returnValue == null) {
            mavContainer.setRequestHandled(true); return;
        }

        HttpServletResponse servletResponse = request.getNativeResponse(HttpServletResponse.class);
        if (servletResponse == null)
            throw new IllegalStateException("No HttpServletResponse");
        ServerHttpResponse serverResponse = new ServletServerHttpResponse(servletResponse);

        if (returnValue instanceof ResponseEntity) {

            serverResponse.setStatusCode(((ResponseEntity<?>) returnValue).getStatusCode());
            serverResponse.getHeaders().putAll(((ResponseEntity<?>) returnValue).getHeaders());

            if ((returnValue = ((ResponseEntity<?>) returnValue).getBody()) == null) {
                mavContainer.setRequestHandled(true);
                serverResponse.flush(); return;
            }

            returnType = returnType.nested();
        }

        ServletRequest servletRequest = request.getNativeRequest(ServletRequest.class);
        if (servletRequest == null)
            throw new IllegalStateException("No ServletRequest");

        TraverseHandler handler;

        Collection<MediaType> mediaTypes = getMediaTypes(request);
        Class<?> elementType = forMethodParameter(returnType).getGeneric().toClass();

        if (mediaTypes.stream().anyMatch(TEXT_EVENT_STREAM::includes) ||
                ServerSentEvent.class.isAssignableFrom(elementType))

            handler = new SseEmitTraverseHandler(request, mavContainer, servletRequest, serverResponse, new SseEmitter(STREAMING_TIMEOUT));
        else if (CharSequence.class.isAssignableFrom(elementType))

            handler = new TextEmitTraverseHandler(request, mavContainer, servletRequest, serverResponse,
                    getEmitter(mediaTypes.stream().filter(MimeType::isConcrete).findFirst().orElse(TEXT_PLAIN)));
        else handler = mediaTypes.stream().flatMap(type -> JSON_STREAMING_MEDIA_TYPES.stream()
                            .filter(streamingType -> streamingType.includes(type)))
                    .findFirst().<TraverseHandler>map(streamingType ->
                            new JsonEmitTraverseHandler(request, mavContainer, servletRequest, serverResponse, getEmitter(streamingType)))
                    .orElseGet(() ->
                            new CollectTraverseHandler(request, mavContainer, serverResponse, servletResponse, elementType));

        warnTaskExecutor(returnType);

        handler.handle((TraverseSupport<?, ?>) returnValue);
    }



    private abstract static class TraverseHandler {

        final NativeWebRequest request; final ModelAndViewContainer mavContainer;

        private TraverseHandler(NativeWebRequest request, ModelAndViewContainer mavContainer) {
            this.request = request; this.mavContainer = mavContainer;
        }

        void handle(TraverseSupport<?, ?> traverseSupport) throws Exception {
            defer(deferred(traverseSupport));
        }

        abstract DeferredResult<?> deferred(TraverseSupport<?, ?> traverseSupport);

        void defer(DeferredResult<?> deferred) throws Exception {
            getAsyncManager(request).startDeferredResultProcessing(deferred, mavContainer);
        }
    }

    private abstract class EmitTraverseHandler<E extends ResponseBodyEmitter> extends TraverseHandler {

        final ServletRequest servletRequest; final ServerHttpResponse serverResponse;

        final E emitter;

        private EmitTraverseHandler(NativeWebRequest request, ModelAndViewContainer mavContainer, ServletRequest servletRequest,
                                    ServerHttpResponse serverResponse, E emitter) {
            super(request, mavContainer);
            this.servletRequest = servletRequest;
            this.serverResponse = serverResponse;
            this.emitter = emitter;
        }

        @Override
        DeferredResult<?> deferred(TraverseSupport<?, ?> traverseSupport) {
            DeferredResult<?> deferred = new DeferredResult<>(emitter.getTimeout());

            Boolean stop = vol(false);
            emitter.onTimeout(() -> {
                if (logger.isTraceEnabled())
                    logger.trace("Request timed out for: " + traverseSupport);
                stop.flg = true; emitter.complete();
            });
            emitter.onError(emitter::completeWithError);
            taskExecutor.execute(() -> {
                try {
                    take(traverseSupport, not(stop::get)).traverse(each(value -> {
                        try {
                            emit(value);
                        } catch (Throwable ex) {
                            if (logger.isTraceEnabled())
                                logger.trace("Send for " + traverseSupport + " failed: " + ex);
                            stop.flg = true;
                        }
                    }));
                    if (!stop.flg) {
                        if (logger.isTraceEnabled())
                            logger.trace("Traverse for: " + traverseSupport + " completed");
                        emitter.complete();
                    }
                } catch (Throwable ex) {
                    if (logger.isTraceEnabled())
                        logger.trace("Traverse for: " + traverseSupport + " failed: " + ex);
                    emitter.completeWithError(ex);
                }
            });

            return deferred;
        }

        @Override
        void defer(DeferredResult<?> deferred) throws Exception {
            emitter.extendResponse(serverResponse);
            disableContentCaching(servletRequest);
            try {
                super.defer(deferred);
            } catch (Throwable ex) {
                emitter.initializeWithError(ex); throw ex;
            }
            emitter.initialize(new HttpMessageConvertingHandler(new DelegatingServerHttpResponse(serverResponse) {

                private final HttpHeaders headers = new HttpHeaders();

                { headers.putAll(serverResponse.getHeaders()); }

                @Override public HttpHeaders getHeaders() {
                    return headers; }
            }, deferred));
        }

        abstract void emit(Object element) throws IOException;
    }

    private class SseEmitTraverseHandler extends EmitTraverseHandler<SseEmitter> {

        private SseEmitTraverseHandler(NativeWebRequest request, ModelAndViewContainer mavContainer, ServletRequest servletRequest,
                                       ServerHttpResponse serverResponse, SseEmitter emitter) {
            super(request, mavContainer, servletRequest, serverResponse, emitter);
        }

        @Override
        protected void emit(Object element) throws IOException {
            if (element instanceof ServerSentEvent)
                emitter.send(sseEvent((ServerSentEvent<?>) element));
            else
                emitter.send(element, APPLICATION_JSON);
        }

        private SseEventBuilder sseEvent(ServerSentEvent<?> sse) {
            SseEventBuilder builder = SseEmitter.event();

            ofNullable(sse.id()).ifPresent(builder::id);
            ofNullable(sse.event()).ifPresent(builder::name);
            ofNullable(sse.data()).ifPresent(builder::data);
            ofNullable(sse.retry()).map(Duration::toMillis)
                    .ifPresent(builder::reconnectTime);
            ofNullable(sse.comment()).ifPresent(builder::comment);

            return builder;
        }
    }

    private class JsonEmitTraverseHandler extends EmitTraverseHandler<ResponseBodyEmitter> {

        private JsonEmitTraverseHandler(NativeWebRequest request, ModelAndViewContainer mavContainer, ServletRequest servletRequest,
                                        ServerHttpResponse serverResponse, ResponseBodyEmitter emitter) {
            super(request, mavContainer, servletRequest, serverResponse, emitter);
        }

        @Override protected void emit(Object element) throws IOException {
            emitter.send(element, APPLICATION_JSON);
            emitter.send("\n", TEXT_PLAIN); }
    }

    private class TextEmitTraverseHandler extends EmitTraverseHandler<ResponseBodyEmitter> {

        private TextEmitTraverseHandler(NativeWebRequest request, ModelAndViewContainer mavContainer, ServletRequest servletRequest,
                                        ServerHttpResponse serverResponse, ResponseBodyEmitter emitter) {
            super(request, mavContainer, servletRequest, serverResponse, emitter);
        }

        @Override protected void emit(Object element) throws IOException {
            emitter.send(element, TEXT_PLAIN); }
    }

    private class CollectTraverseHandler extends TraverseHandler {

        final ServerHttpResponse serverResponse; final HttpServletResponse servletResponse;

        final Class<?> elementType;

        private CollectTraverseHandler(NativeWebRequest request, ModelAndViewContainer mavContainer,
                                       ServerHttpResponse serverResponse, HttpServletResponse servletResponse,
                                       Class<?> elementType) {
            super(request, mavContainer);
            this.serverResponse = serverResponse;
            this.servletResponse = servletResponse;
            this.elementType = elementType;
        }

        @Override
        DeferredResult<?> deferred(TraverseSupport<?, ?> traverseSupport) {
            DeferredResult<Object> deferred = new DeferredResult<>();

            Boolean stop = vol(false); deferred.onTimeout(() -> {
                if (logger.isTraceEnabled())
                    logger.trace("Request timed out for: " + traverseSupport);
                stop.flg = true;
            });
            taskExecutor.execute(() -> {
                try {
                    List<?> collected = take(traverseSupport, not(stop::get)).traverse(toList());
                    if (!stop.flg) {
                        if (logger.isTraceEnabled())
                            logger.trace("Traverse for: " + traverseSupport + " completed");
                        deferred.setResult(collected.isEmpty() && Void.class == elementType ? null : collected);
                    }
                } catch (Throwable ex) {
                    if (logger.isTraceEnabled())
                        logger.trace("Traverse for: " + traverseSupport + " failed: " + ex);
                    deferred.setErrorResult(ex);
                }
            });

            return deferred;
        }

        @Override
        void defer(DeferredResult<?> deferred) throws Exception {
            super.defer(deferred);
            serverResponse.getHeaders().forEach((headerName, headerValues) ->
                    headerValues.forEach(headerValue ->
                            servletResponse.addHeader(headerName, headerValue)));
        }
    }

    @SuppressWarnings("unchecked")
    private Collection<MediaType> getMediaTypes(NativeWebRequest request) throws HttpMediaTypeNotAcceptableException {

        Collection<MediaType> mediaTypes = (Collection<MediaType>) request.getAttribute(
                PRODUCIBLE_MEDIA_TYPES_ATTRIBUTE, SCOPE_REQUEST);

        return mediaTypes == null || mediaTypes.isEmpty()
                ? contentNegotiationManager.resolveMediaTypes(request) : mediaTypes;
    }

    private ResponseBodyEmitter getEmitter(MediaType mediaType) {
        return new ResponseBodyEmitter(STREAMING_TIMEOUT) {
            @Override protected void extendResponse(ServerHttpResponse outputMessage) {
                outputMessage.getHeaders().setContentType(mediaType); } };
    }

    /**
     * A {@link Handler} that writes to the response using HttpMessageConverter's.
     */
    private class HttpMessageConvertingHandler implements Handler {

        final ServerHttpResponse serverResponse; final DeferredResult<?> deferred;

        HttpMessageConvertingHandler(ServerHttpResponse serverResponse, DeferredResult<?> deferred) {
            this.serverResponse = serverResponse; this.deferred = deferred;
        }

        @Override @SuppressWarnings("unchecked") public void send(Object data, MediaType mediaType) throws IOException {
            converters.stream().filter(converter -> converter.canWrite(data.getClass(), mediaType))
                    .findFirst().map(converter -> (HttpMessageConverter<Object>) converter)
                    .orElseThrow(() -> new IllegalArgumentException("No suitable converter for " + data.getClass()))
                    .write(data, mediaType, serverResponse); }

        @Override public void complete() {
            try { serverResponse.flush(); deferred.setResult(null); }
            catch (IOException ex) { deferred.setErrorResult(ex); } }
        @Override public void completeWithError(Throwable failure) {
            deferred.setErrorResult(failure); }
        @Override public void onTimeout(Runnable callback) {
            deferred.onTimeout(callback); }
        @Override public void onError(Consumer<Throwable> callback) {
            deferred.onError(callback); }
        @Override public void onCompletion(Runnable callback) {
            deferred.onCompletion(callback); }
    }

    private List<HttpMessageConverter<?>> ensureTextPlainWrite(List<HttpMessageConverter<?>> converters) {
        for (HttpMessageConverter<?> converter : converters)
            if (converter.canWrite(String.class, TEXT_PLAIN))
                return converters;
        List<HttpMessageConverter<?>> result = new ArrayList<>(converters.size() + 1);
        result.add(new StringHttpMessageConverter(StandardCharsets.UTF_8));
        result.addAll(converters);
        return result;
    }

    private void warnTaskExecutor(MethodParameter returnType) {
        if (logger.isWarnEnabled() && warnTaskExecutor.compareAndSet(true, false)) {
            logger.warn(format("\n!!! Warning !!!\n" +
                            "Streaming through Traversable or Emittable types requires a TaskExecutor for writing to the response.\n" +
                            "The current %s in use may not be suitable under high load.\n" +
                            "Please configure a TaskExecutor in the MVC config under \"async support\".\n" +
                            "-------------------------------\n" +
                            "Component: %s - " + "%s\n" +
                            "Returning: %s\n" +
                            "!!!",
                    taskExecutor.getClass().getSimpleName(),
                    returnType.getContainingClass().getName(),
                    returnType.getMethod(),
                    forMethodParameter(returnType)));
        }
    }
}
