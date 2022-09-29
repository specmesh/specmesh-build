package io.specmesh.sample;

import com.hashicorp.cdktf.Testing;
import com.microsoft.terraform.TerraformClient;
import com.microsoft.terraform.TerraformOptions;
import imports.kafka.Topic;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test executes the full Spec Mesh lifecycle:
 * - Starts in-memory docker compose Zookeeper / Kafka
 * - Generates Terraform from spec
 * - Creates resources using TF apply wrapper
 */
@Slf4j
@Testcontainers
public class SampleApplicationMainTest {

    private final MainStack stack = new MainStack(Testing.app(), "stack");
    private final String synthesized = Testing.synth(stack);

    public static final String TERRAFORM_INFRASTRUCTURE_TF = "infrastructure.tf.json";

    /* This folder and the files created in it will be deleted after
     * tests are run, even in the event of failures or exceptions.
     */
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    File terraformConfig;

    @Test
    void shouldContainKafkaTopicAndPrintSynth() {
        System.out.println(synthesized);
        assertTrue(Testing.toHaveResource(synthesized, Topic.TF_RESOURCE_TYPE));
    }

    private static final String DOCKER_HOST;
    // Waits for Schema Registry which is the last container to start
    @Container
    public static DockerComposeContainer environment = new DockerComposeContainer(new File("src/test/resources/docker-compose.yml"))
            .withExposedService("zookeeper", 2181)
            .waitingFor("broker", Wait.forLogMessage(".*Alive brokers.*1.*", 1)
                    .withStartupTimeout(Duration.ofSeconds(180)));

    static {
        DOCKER_HOST = System.getenv("CI_JOB_TOKEN") != null ? "docker" : "localhost";
    }

    /* executed before every test: create temporary files */
    @BeforeEach
    public void setUp() {
        try {
            folder.create();
            terraformConfig = folder.newFile(TERRAFORM_INFRASTRUCTURE_TF);
            createTerraformSyncFile();
        }
        catch( IOException ioe ) {
            System.err.println(
                    "error creating temporary test file in " +
                            this.getClass().getSimpleName() );
        }
    }

    private void createTerraformSyncFile() {
        //write the Terraform configuration to a file
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(terraformConfig);
        } catch (IOException e) {
            System.err.println(
                    "error creating Terraform synthesized file in " +
                            this.getClass().getSimpleName() );
        }
        PrintWriter printWriter = new PrintWriter(fileWriter);
        printWriter.print(synthesized);
        printWriter.close();
    }

    @AfterEach
    void tearDown() {
        folder.delete();
    }

    @Test
    void runStack() throws Exception {
        TerraformOptions options = new TerraformOptions();
        try (TerraformClient client = new TerraformClient(options)) {
            client.setOutputListener(System.out::println);
            client.setErrorListener(System.err::println);

            client.setWorkingDirectory(folder.getRoot());
            client.plan().get();
            client.apply().get();
        }
    }
}