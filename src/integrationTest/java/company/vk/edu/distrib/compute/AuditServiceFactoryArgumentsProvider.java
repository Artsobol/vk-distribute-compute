package company.vk.edu.distrib.compute;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.ParameterDeclarations;

import java.util.Set;
import java.util.stream.Stream;

import static org.junit.platform.commons.util.ReflectionUtils.newInstance;

public class AuditServiceFactoryArgumentsProvider implements ArgumentsProvider {
    private final Set<ImmutablePair<Class<? extends KVServiceFactory>, Class<? extends AuditServiceFactory>>>
            factories = Set.of(
    //        ImmutablePair.of(DummyKVServiceFactory.class, AuditServiceFactory.class)
    );

    @Override
    @NonNull
    public Stream<? extends Arguments> provideArguments(
            @NonNull ParameterDeclarations parameters,
            @NonNull ExtensionContext context
    ) {
        return factories.stream()
                .map(it -> Arguments.of(newInstance(it.left), newInstance(it.right)));
    }
}
