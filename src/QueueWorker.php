<?php

namespace PhpRedisQueue;

use Psr\Log\LoggerInterface;

class QueueWorker
{
  protected $defaultConfig = [
    'logger' => null,              // instance of Psr\Log\LoggerInterface
    'processedQueueLimit' => 5000, // pass -1 for no limit
  ];

  protected $config = [];

  public string $queueName;

  /**
   * Name of list that stores jobs that are currently being processed
   * @var string
   */
  public string $processingQueue = 'queue:processing';

  /**
   * Name of list that stores jobs that have been succcessfully procesed
   * @var string
   */
  public string $successQueue = 'queue:processed:success';

  /**
   * Name of list that stores jobs that have been procesed, but failed.
   * This list can be checked via a cron job (or some other method)
   * to rerun the jobs, if you choose.
   * @var string
   */
  public string $failedQueue = 'queue:processsed:failed';

  protected array $callbacks = [];

  /**
   * @param \Predis\Client $redis
   * @param string $queueName
   */
  public function __construct(protected \Predis\Client $redis, string $queueName, array $config = [])
  {
    $this->queueName = 'queue:' . $queueName;
    $this->config = array_merge($this->defaultConfig, $config);

    if (isset($this->config['logger']) && !$this->config['logger'] instanceof LoggerInterface) {
      throw new \InvalidArgumentException('Logger must be an instance of Psr\Log\LoggerInterface.');
    }
  }

  public function work()
  {
    while($jsonData = $this->redis->blmove($this->queueName, $this->processingQueue, 'LEFT', 'LEFT', 0)) {

      $data = json_decode($jsonData);

      if (!isset($this->callbacks[$data->meta->jobName])) {
        $this->log('warning', 'No callback set for job', ['context' => $data]);
        continue;
      }

      // perform the work
      $success = true;

      try {
        // add context (if any) for the dashboard
        $data->context = call_user_func($this->callbacks[$data->meta->jobName], $data->job);
      } catch (\Throwable $e) {
        $success = false;

        // add exceptpion context for the dashboard
        $data->context = $this->getExceptionData($e);

        // log the error for debugging
        $this->log('warning', 'Queue job failed', ['data' => $data]);
      }

      // add to appropiate processed list (success or fail)
      $queue = $success === true ? $this->successQueue : $this->failedQueue;
      $this->redis->lpush($queue, json_encode($data));

      // trim processed lists to keep them tidy
      if ($this->config['processedQueueLimit'] > -1) {
        $this->redis->ltrim($queue, 0, $this->config['processedQueueLimit']);
      }

      // remove from processing queue
      $removed = $this->redis->lrem($this->processingQueue, 1, $jsonData);

      // call onComplete callback

      $callbackName = $data->meta->jobName . '_complete';
      if (isset($this->callbacks[$callbackName])) {
        try {
          call_user_func($this->callbacks[$callbackName], $success, $data);
        } catch (\Throwable $e) {
          $this->log('warning', 'OnComplete callback for completed queued job failed', [
            'context' => [
              'exception' => $this->getExceptionData($e),
              'data' => $data,
              'job_success' => $success,
            ]
          ]);
        }
      }

      // wait a second before checking the queue again
      sleep(1);
    }
  }

  /**
   * Add a callback for a specific job.
   * @param string $jobName
   * @param callable $callable {
   *   @param object $object      Top-level properties are `meta` and `job`. Meta contains information such as
   *                              the name of the job, queue it was placed in, ID, and datetime the job was
   *                              initialized. Job contains the data that was passed along with the job.
   *   @return array              ['success' => TRUE/FALSE, 'context' => array of k => v pairs]
   *                              Context data will be added to element in the processed:fail queue (if failed)
   *                              or the processed:success queue (if successful).
   * @return void
   */
  public function addCallback(string $jobName, callable $callable)
  {
    $this->callbacks[$jobName] = $callable;
  }

  /**
   * Add a callback for a specific job when it completes, not matter if it was successful or failed.
   * @param string $jobName
   * @param callable $callable {
   *   @param bool   $success     Was the job successful?
   *   @param object $object      Top-level properties that are always present are `meta`, `context` and `job`.
   *                              Meta contains information such as the name of the job, queue it was placed in, ID,
   *                              and datetime the job was initialized. Job contains the data that was passed along
   *                              with the job. Context contains data returned from the callback function that
   *                              performs the work (added via QueueWorker::addCallback()
   *   @return void
   *                              Context data will be added to element in the processed:fail queue (if failed)
   *                              or the processed:success queue (if successful).
   * @return void
   */
  public function addOnCompleteCallback(string $jobName, callable $callable)
  {
    $callbackName = $jobName . '_complete';
    $this->callbacks[$callbackName] = $callable;
  }

  protected function getExceptionData(\Throwable $e)
  {
    return [
      'exception_type' => get_class($e),
      'exception_code' => $e->getCode(),
      'exception_message' => $e->getMessage(),
      'exception_file' => $e->getFile(),
      'exception_line' => $e->getLine(),
    ];
  }

  protected function log(string $level, string $message, array $data = [])
  {
    if (!isset($this->config['logger'])) {
      return;
    }

    $this->config['logger']->$level($message, $data);
  }
}
