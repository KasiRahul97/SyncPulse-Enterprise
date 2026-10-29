package com.syncpulse.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.scripting.support.ResourceScriptSource;

import java.util.List;

@Configuration
public class RedisConfig {

    /**
     * The atomic debounce script. Spring registers it (SCRIPT LOAD) and invokes it
     * with EVALSHA, so the decision is made entirely inside Redis under one atomic
     * execution. Returns a heterogeneous array, hence {@code List} as the result type.
     */
    @Bean
    @SuppressWarnings({"rawtypes", "unchecked"})
    public RedisScript<List> debounceScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(new ClassPathResource("redis/debounce.lua")));
        script.setResultType(List.class);
        return script;
    }
}
