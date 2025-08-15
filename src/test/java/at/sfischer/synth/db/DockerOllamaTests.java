package at.sfischer.synth.db;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;


public interface DockerOllamaTests {

    Logger LOGGER = LoggerFactory.getLogger(DockerOllamaTests.class);

    String URL = "http://localhost:11434/api/chat";
    String MODEL = "llama3.1";

    @BeforeAll
    static void setupOllama() throws InterruptedException {
        DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder().build();

        try (DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                .dockerHost(config.getDockerHost())
                .sslConfig(config.getSSLConfig())
                .maxConnections(100)
                .connectionTimeout(Duration.ofSeconds(30))
                .responseTimeout(Duration.ofSeconds(45))
                .build()) {

            DockerClient dockerClient = DockerClientImpl.getInstance(config, httpClient);

            List<Container> containers = dockerClient.listContainersCmd()
                    .withShowAll(true)
                    .exec();

            LOGGER.info("Checking docker container");
            Optional<Container> ollamaContainer = containers.stream()
                    .filter(c -> Arrays.asList(c.getNames()).contains("/ollama"))
                    .findFirst();

            boolean exists = ollamaContainer.isPresent();
            LOGGER.info("Docker container exists: {}", exists);
            if(exists){
                Container container = ollamaContainer.get();
                if(!container.getState().equalsIgnoreCase("running")){
                    LOGGER.info("Start docker container");
                    dockerClient.startContainerCmd(container.getId()).exec();
                }
            } else {
                LOGGER.info("Pulling Ollama Docker image");

                dockerClient.pullImageCmd("ollama/ollama")
                        .withTag("latest")
                        .exec(new PullImageResultCallback())
                        .awaitCompletion();

                LOGGER.info("Creating Ollama container");

                Ports portBindings = new Ports();
                ExposedPort containerPort = ExposedPort.tcp(11434);
                Ports.Binding hostPort = Ports.Binding.bindPort(11434);

                portBindings.bind(containerPort, hostPort);

                HostConfig hostConfig = HostConfig.newHostConfig()
                        .withPortBindings(new PortBinding(Ports.Binding.bindPort(11434), new ExposedPort(11434)))
                        .withBinds(new Bind("ollama", new Volume("/root/.ollama")))
                        .withRuntime("nvidia"); // --gpus=all

                CreateContainerResponse container = dockerClient.createContainerCmd("ollama/ollama")
                        .withName("ollama")
                        .withHostConfig(hostConfig)
                        .exec();

                LOGGER.info("Start new docker container");
                dockerClient.startContainerCmd(container.getId()).exec();

                LOGGER.info("Pulling Ollama model: " + MODEL);
                String[] command = {"ollama", "pull", MODEL};
                ExecCreateCmdResponse execCreateCmdResponse = dockerClient.execCreateCmd(container.getId())
                        .withAttachStdout(true)
                        .withAttachStderr(true)
                        .withCmd(command)
                        .exec();

                dockerClient.execStartCmd(execCreateCmdResponse.getId())
                        .exec(new ResultCallback.Adapter<Frame>() {
                            @Override
                            public void onNext(Frame frame) {
                                switch (frame.getStreamType()) {
                                    case STDOUT:
                                        System.out.print(new String(frame.getPayload(), StandardCharsets.UTF_8));
                                        break;
                                    case STDERR:
                                        System.err.print(new String(frame.getPayload(), StandardCharsets.UTF_8));
                                        break;
                                    default:
                                        break;
                                }
                            }

                            @Override
                            public void onComplete() {
                                System.out.println("\nModel pull completed.");
                                super.onComplete();
                            }
                        })
                        .awaitCompletion();
            }



        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
