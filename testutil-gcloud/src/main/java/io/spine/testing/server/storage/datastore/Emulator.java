package io.spine.testing.server.storage.datastore;


import com.google.cloud.NoCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.datastore.DatastoreOptions;
import io.spine.server.storage.datastore.ProjectId;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.testcontainers.containers.DatastoreEmulatorContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Manages a Docker-powered Datastore Emulator.
 */
@SuppressWarnings("resource")
public final class Emulator {

    private static final DockerImageName IMAGE =
            DockerImageName.parse("gcr.io/google.com/cloudsdktool/google-cloud-cli:441.0.0-emulators");

    private static @MonotonicNonNull DatastoreEmulatorContainer emulator = null;
    private static @MonotonicNonNull DatastoreOptions options = null;

    /**
     * Prevents the direct instantiation.
     */
    private Emulator() {
    }

    /**
     * Returns the connection options to the currently running Datastore Emulator.
     *
     * <p>If no emulator is currently running, a new instance is launched via Docker.
     */
    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    public static synchronized DatastoreOptions options(ProjectId projectId, int port) {
        if (options == null) {
            if (emulator == null) {
                System.out.println("---- Starting a new Datastore Emulator via Docker...");
                emulator = new Emulator.Container(IMAGE, projectId, port);
                emulator.start();
            }

            options = DatastoreOptions.newBuilder()
                    .setHost(emulator.getEmulatorEndpoint())
                    .setCredentials(NoCredentials.getInstance())
                    .setRetrySettings(ServiceOptions.getNoRetrySettings())
                    .setProjectId(emulator.getProjectId())
                    .build();
        }
        return options;
    }

    /**
     * A container running the Emulator image.
     *
     * <p>The container is started in its "full consistency" mode.
     */
    private static class Container extends DatastoreEmulatorContainer {

        /**
         * A template of BASH command to execute upon Docker container start.
         */
        private static final String TEMPLATE =
                "gcloud beta emulators datastore start " +
                        "--project %s " +
                        "--host-port 0.0.0.0:%d " +
                        "--consistency 1.0";

        private final ProjectId projectId;
        private final int port;

        /**
         * Creates a new Emulator container.
         *
         * <p>Remember, the instance must be {@linkplain #start() started} after creation.
         *
         * @param dockerImageName reference to the Docker image
         * @param projectId       the GCP project ID
         * @param port            port to start the emulator on
         */
        private Container(DockerImageName dockerImageName, ProjectId projectId, int port) {
            super(dockerImageName);
            this.projectId = projectId;
            this.port = port;
        }

        @Override
        protected void configure() {
            var command = String.format(TEMPLATE, projectId.value(), port);
            withCommand("/bin/sh", "-c", command);
        }

        @Override
        public String getEmulatorEndpoint() {
            return getHost() + ':' + getMappedPort(port);
        }

        @Override
        public String getProjectId() {
            return projectId.value();
        }
    }
}
