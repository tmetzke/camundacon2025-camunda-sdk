package org.example;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.response.ActivatedJob;
import io.camunda.client.api.search.enums.UserTaskState;
import io.camunda.client.api.search.response.SearchResponse;
import io.camunda.client.api.search.response.UserTask;
import io.camunda.client.api.worker.JobClient;
import io.camunda.client.api.worker.JobWorker;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class App {
  public static void main(String[] args) {
    try (
        // connect locally, no authentication
        final var client = CamundaClient.newClientBuilder().build();
        // create a listener for user task creation (NEW)
        final var taskListener = createTaskListener(client);
        // listen to process instance completion (NEW)
        final var processEndListener = createProcessEndListener(client)) {
      // direct engine interaction (formerly Zeebe client)
      final var processDefinitionKey = deployProcessDefinition(client);
      // direct engine interaction (formerly Zeebe client)
      final var processInstanceKey = startProcessInstance(client, processDefinitionKey);
      // secondary storage interaction (formerly Tasklist client)
      printUserTaskDetails(client, processInstanceKey);
    }
  }

  private static long deployProcessDefinition(CamundaClient client) {
    return client
        .newDeployResourceCommand()
        .addResourceFromClasspath("my-process.bpmn")
        .execute()
        .getProcesses().getFirst()
        .getProcessDefinitionKey();
  }

  private static long startProcessInstance(CamundaClient client, long processDefinitionKey) {
    return client
        .newCreateInstanceCommand()
        .processDefinitionKey(processDefinitionKey)
        .execute()
        .getProcessInstanceKey();
  }

  private static JobWorker createTaskListener(CamundaClient client) {
    return client
        .newWorker()
        .jobType("task-created-listener")
        .handler((jobClient, job) ->
            completeUserTask(jobClient, job, client))
        .open();
  }

  private static void completeUserTask(JobClient jobClient, ActivatedJob job, CamundaClient client) {
    final var userTask = job.getUserTask();
    if (userTask != null) {
      final var userTaskKey = userTask.getUserTaskKey();
      System.out.println("User task created with key: " + userTaskKey);
      // complete the user task listener (NEW)
      jobClient
          .newCompleteCommand(job.getKey())
          .withResult(result -> result.forUserTask().correctAssignee("john"))
          .execute();
      System.out.println("User task listener completed");
      // complete the user task (formerly Tasklist client)
      client
          .newCompleteUserTaskCommand(userTaskKey)
          .variables(Map.of("han", "bar"))
          .execute();
      System.out.println("User task completed");
    }
  }

  private static JobWorker createProcessEndListener(CamundaClient client) {
    return client
        .newWorker()
        .jobType("process-ended-listener")
        .handler(App::extractProcessVariables)
        .open();
  }

  private static void extractProcessVariables(JobClient jobClient, ActivatedJob job) {
    System.out.println("Process instance with key ended: " + job.getProcessInstanceKey());
    System.out.println("Process instance variables: " + job.getVariables());
    jobClient.newCompleteCommand(job.getKey());
  }

  private static void printUserTaskDetails(CamundaClient client, long processInstanceKey) {
    final var userTasks = getUserTasks(client, processInstanceKey);
    if (userTasks.isEmpty()) {
      System.out.println("No user tasks found");
    } else {
      final var userTask = userTasks.getFirst();
      System.out.println("User Task assignee is: " + userTask.getAssignee());
      System.out.println("User Task state is: " + userTask.getState());
    }
  }

  private static List<UserTask> getUserTasks(CamundaClient client, long processInstanceKey) {
    try {
      int retryCount = 0;
      while (retryCount < 10) {
        final var userTasks = searchUserTasks(client, processInstanceKey);
        if (!userTasks.items().isEmpty()) {
          return userTasks.items();
        } else {
          retryCount++;
          System.out.println("No user tasks found yet, retrying in 2 seconds");
          // handle eventual consistency
          Thread.sleep(Duration.ofSeconds(2).toMillis());
        }
      }
      return Collections.emptyList();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static SearchResponse<UserTask> searchUserTasks(CamundaClient client, long processInstanceKey) {
    // search for user tasks in secondary storage
    return client
        .newUserTaskSearchRequest()
        .filter(f -> f
            .processInstanceKey(processInstanceKey)
            .assignee(a -> a.exists(true))
            .state(s -> s.neq(UserTaskState.CREATED)))
        .execute();
  }
}