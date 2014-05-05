package org.eduonix;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by ubu on 5/4/14.
 */
public class SeismicCluster {

    private static final Boolean termination_Protection= true;
    private static final int NUM_BOX = 1;
    private static final String BOX = InstanceType.M1Medium.toString();
    private static final String BASE_URL = "s3n://eduonix";
    private static final String JAR = BASE_URL+"/job/MapReduceModule.jar";

    private static final List<JobFlowExecutionState> states= Arrays
            .asList(new JobFlowExecutionState[]{JobFlowExecutionState.COMPLETED, JobFlowExecutionState.FAILED,
                    JobFlowExecutionState.TERMINATED});

    static AmazonElasticMapReduceClient client;

    //  jar configured in pom jar plugin
    // <mainClass>org.eduonix.SeismicProcessor</mainClass>

    public static void main(String[] args) throws Exception {

        SeismicCluster cluster = new SeismicCluster();
        cluster.init();
        cluster.runCluster();
    }




    void init() throws Exception {
        AWSCredentials credentials = new PropertiesCredentials(
                SeismicCluster.class.getClassLoader().getResourceAsStream("AwsCredentials.properties"));
        client = new AmazonElasticMapReduceClient(credentials);
        client.setRegion(Region.getRegion(Regions.EU_WEST_1));
    }


    public static JobFlowInstancesConfig setNodeBoxs() throws Exception {

        // set up instances
        JobFlowInstancesConfig box = new JobFlowInstancesConfig();
        box.setHadoopVersion( "2.2.0");
        box.setInstanceCount(NUM_BOX);
        box.setTerminationProtected(termination_Protection);
        box.setMasterInstanceType(BOX);
        box.setSlaveInstanceType(BOX);

        return box;
    }


    public void runCluster() throws Exception {
        // Configure the job flow
        RunJobFlowRequest request = new RunJobFlowRequest("seismicProcessor", setNodeBoxs());
        request.setLogUri("s3n://eduonix/log/");
        HadoopJarStepConfig jarConfig = new HadoopJarStepConfig(JAR);

        try {

            StepConfig enableDebugging = new StepConfig()
                    .withName("Enable debugging")
                    .withActionOnFailure("TERMINATE_JOB_FLOW")
                    .withHadoopJarStep(new StepFactory().newEnableDebuggingStep());

            StepConfig runJar =
                    new StepConfig("/eduonix/job/MapReduceModule.jar", jarConfig);

            request.setSteps(Arrays.asList(new StepConfig[]{enableDebugging, runJar}));

            //Run the job flow
            RunJobFlowResult result = client.runJobFlow(request);

            //Check the status of the running job
            String lastState = "";

            STATUS_LOOP:
            while (true) {
                DescribeJobFlowsRequest desc =
                        new DescribeJobFlowsRequest(
                                Arrays.asList(new String[]{result.getJobFlowId()}));
                DescribeJobFlowsResult descResult = client.describeJobFlows(desc);
                for (JobFlowDetail detail : descResult.getJobFlows()) {
                    String state = detail.getExecutionStatusDetail().getState();
                    if (isDone(state)) {
                        System.out.println("Job " + state + ": " + detail.toString());
                        break STATUS_LOOP;
                    } else if (!lastState.equals(state)) {
                        lastState = state;
                        System.out.println("Job " + state + " at " + new Date().toString());
                    }
                }
                Thread.sleep(10000);
            }
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
    }

    public static boolean isDone(String value) {
        JobFlowExecutionState state = JobFlowExecutionState.fromValue(value);
        return states.contains(state);
    }

}
