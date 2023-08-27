package io.spine.testing.server.storage.datastore;


import com.google.cloud.datastore.DatastoreOptions;
import io.spine.server.storage.datastore.ProjectId;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages a Docker-powered Datastore Emulator.
 *
 * @implNote This class acts as a JVM-level singleton, operating in {@code static} context,
 * to make the state shared across all launched JUnit tests. For each project ID,
 * a new container is launched. Once the Java process, from which the container was started, dies,
 * corresponding Docker container is automatically stopped.
 */
final class Emulator {

    private static final DockerImageName IMAGE =
            DockerImageName.parse("gcr.io/google.com/cloudsdktool/google-cloud-cli:441.0.0-emulators");


    /**
     * Currently running emulators, per project ID with which each of them is running.
     */
    private static final Map<ProjectId, EmulatorContainer> containers = new HashMap<>();

    /**
     * Prevents the direct instantiation.
     */
    private Emulator() {
    }

    /**
     * Returns the connection options to the running Datastore Emulator.
     *
     * <p>In case there is already an emulator running with the passed project ID,
     * returns the connection options to that emulator.
     *
     * <p>Otherwise, a new instance is launched via Docker.
     *
     * @param projectId Datastore project ID, which may have an arbitrary value,
     *                  and not correspond to any real GCP project
     * @param port      port in a Docker container machine, through which the emulator is going to be accessed
     */
    static synchronized DatastoreOptions at(ProjectId projectId, int port) {
        var emulator = containers.get(projectId);
        if (emulator == null) {
            emulator = new EmulatorContainer(IMAGE, projectId, port);
            emulator.startAndServe();
            containers.put(projectId, emulator);
        }
        return emulator.options();
    }
}
