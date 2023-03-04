import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;
import software.amazon.awssdk.services.ec2.model.InstanceType;

public class Deployment {

//Out of the box skeleton code need to be modified  to our needs.
    public static  void main(String[] args)  {
        //TODO: change bucket-name accordingly.
        String BucketName = "s3://ds-3-files-amit/";
        //step1
        String input_1 = BucketName + "inputBiarcs/";
        String output_1 = BucketName + "NounPatternCount/";
        String jar_1 = "JarFiles/MapperReducer1.jar";
        //step2
        String input_2 = output_1;
        String output_2 = BucketName + "DPMINOutput/";
        String jar_2 = "JarFiles/DPMIN.jar";
        //step3
        String input_3 = output_2;
        String output_3 = BucketName + "PatternIndexerOutput/";
        String jar_3 = "JarFiles/PatternIndexer.jar";
        //step4
        String input_4_1 = output_1;
        String input_4_2 = output_3;
        String output_4 = BucketName + "JoinOutput/";
        String jar_4 = "JarFiles/Join.jar";
        //step5
        String input_5 = output_4;
        String output_5 = BucketName + "Nouns/";
        String jar_5 = "JarFiles/Nouns.jar";
        //step6
        String input_6_1 = output_5;
        String input_6_2 = BucketName + "hypernym.txt";
        String output_6 = BucketName + "LabelsOutput/";
        String jar_6 = "JarFiles/Labels.jar";

        EmrClient emr = EmrClient.builder().build();
        HadoopJarStepConfig hadoopJarStepConfig_1 = HadoopJarStepConfig.builder()
                .jar(BucketName + jar_1)
                .args(input_1, output_1)
                .build();
        StepConfig step_1 = StepConfig.builder()
                .name("Step_1")
                .hadoopJarStep(hadoopJarStepConfig_1)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
        HadoopJarStepConfig hadoopJarStepConfig_2 = HadoopJarStepConfig.builder()
                .jar(BucketName + jar_2)
                .args(input_2, output_2)
                .build();
        StepConfig step_2 = StepConfig.builder()
                .name("Step_2")
                .hadoopJarStep(hadoopJarStepConfig_2)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
        HadoopJarStepConfig hadoopJarStepConfig_3 = HadoopJarStepConfig.builder()
                .jar(BucketName + jar_3)
                .args(input_3, output_3)
                .build();
        StepConfig step_3 = StepConfig.builder()
                .name("Step_3")
                .hadoopJarStep(hadoopJarStepConfig_3)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
        HadoopJarStepConfig hadoopJarStepConfig_4 = HadoopJarStepConfig.builder()
                .jar(BucketName + jar_4)
                .args(input_4_1, input_4_2, output_4)
                .build();
        StepConfig step_4 = StepConfig.builder()
                .name("Step_4")
                .hadoopJarStep(hadoopJarStepConfig_4)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
        HadoopJarStepConfig hadoopJarStepConfig_5 = HadoopJarStepConfig.builder()
                .jar(BucketName + jar_5)
                .args(input_5, output_5)
                .build();
        StepConfig step_5 = StepConfig.builder()
                .name("Step_5")
                .hadoopJarStep(hadoopJarStepConfig_5)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
        HadoopJarStepConfig hadoopJarStepConfig_6 = HadoopJarStepConfig.builder()
                .jar(BucketName + jar_6)
                .args(input_6_1, input_6_2, output_6)
                .build();
        StepConfig step_6 = StepConfig.builder()
                .name("Step_6")
                .hadoopJarStep(hadoopJarStepConfig_6)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
        RunJobFlowRequest request = RunJobFlowRequest.builder()
                .name("AmitAndTomCluster")
                .instances(JobFlowInstancesConfig.builder()
                        .ec2KeyName("amit_tom")
                        .instanceCount(7)
                        .masterInstanceType(InstanceType.M5_XLARGE.toString())
                        .slaveInstanceType(InstanceType.M5_XLARGE.toString())
                        .keepJobFlowAliveWhenNoSteps(false)
                        .placement(PlacementType.builder().availabilityZone("us-east-1a").build())
                        .build())
                .logUri("s3://ds-3-files-amit/logs/")
                .steps(step_1, step_2, step_3, step_4, step_5, step_6)
                .releaseLabel("emr-5.36.0")
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole").build();


        RunJobFlowResponse response = emr.runJobFlow(request);
        String jobFlowId = response.jobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

    }




}
