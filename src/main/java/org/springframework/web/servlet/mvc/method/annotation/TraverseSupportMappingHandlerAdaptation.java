package org.springframework.web.servlet.mvc.method.annotation;

import org.moodminds.emission.Emittable;
import org.moodminds.traverse.Traversable;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.web.accept.ContentNegotiationManager;
import org.springframework.web.method.support.HandlerMethodReturnValueHandler;

import java.util.ArrayList;
import java.util.List;

import static java.util.Optional.ofNullable;

/**
 * The {@link Emittable} and {@link Traversable} request mapping adaptation configuration bean.
 */
@Configuration
public class TraverseSupportMappingHandlerAdaptation implements InitializingBean {

    private final RequestMappingHandlerAdapter requestMappingHandlerAdapter;
    private final TaskExecutor taskExecutor;
    private final ContentNegotiationManager contentNegotiationManager;

    /**
     * Construct the configuration object with the specified {@link RequestMappingHandlerAdapter}.
     *
     * @param requestMappingHandlerAdapter the specified {@link RequestMappingHandlerAdapter}
     */
    public TraverseSupportMappingHandlerAdaptation(RequestMappingHandlerAdapter requestMappingHandlerAdapter,
                                                   TaskExecutor taskExecutor, ContentNegotiationManager contentNegotiationManager) {
        this.requestMappingHandlerAdapter = requestMappingHandlerAdapter;
        this.taskExecutor = taskExecutor;
        this.contentNegotiationManager = contentNegotiationManager;
    }

    /**
     * Register the value handler in the {@link RequestMappingHandlerAdapter}.
     */
    @Override
    public void afterPropertiesSet() {
        ofNullable(requestMappingHandlerAdapter.getReturnValueHandlers()).ifPresent(handlers -> handlers.stream()
                .filter(h -> h instanceof ResponseBodyEmitterReturnValueHandler)
                .map(h -> (ResponseBodyEmitterReturnValueHandler) h)
                .findFirst().ifPresent(emitterHandler -> {
                    List<HandlerMethodReturnValueHandler> customHandlers = new ArrayList<>(handlers.size() + 1);
                    customHandlers.add(new ResponseBodyTraverseSupportReturnValueHandler(
                            taskExecutor, requestMappingHandlerAdapter.getMessageConverters(), contentNegotiationManager));
                    customHandlers.addAll(handlers);
                    requestMappingHandlerAdapter.setReturnValueHandlers(customHandlers);
                }));
    }
}
