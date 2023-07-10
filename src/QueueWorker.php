<?php

namespace PhpRedisQueue;

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

    if (isset($this->config['logger']) && !$this->config['logger'] instanceof \Psr\Log\LoggerInterface) {
      throw new \InvalidArgumentException('Logger must be an instance of Psr\Log\LoggerInterface.');
    }
  }

  public function work()
  {
    while($jsonData = $this->redis->blmove($this->queueName, $this->processingQueue, 'LEFT', 'LEFT', 0)) {

      $data = json_decode($jsonData);

      if (!isset($this->callbacks[$data->job])) {
        $this->log('warning', 'No callback set for job', ['context' => $data]);
        continue;
      }

      $response = call_user_func($this->callbacks[$data->job], $data);

      // add response to original request
      $data->response = $response;

      // add to appropiate processed list (success or fail)
      $queue = $response['success'] === true ? $this->successQueue : $this->failedQueue;
      $this->redis->lpush($queue, json_encode($data));

      // trim processed lists
      if ($this->config['processedQueueLimit'] > -1) {
        $redis->ltrim($queue, 0, $this->config['processedQueueLimit']);
      }

      // if the job failed, log it
      if ($response['success'] === false) {
        $this->log('warning', 'Queue job failed', ['context' => $data]);
      }

      // remove from processing queue
      $removed = $this->redis->lrem($this->processingQueue, 1, $jsonData);

      // wait a second before checking the queue again
      sleep(1);
    }
  }

  /**
   * @param string $jobName
   * @param callable $callable Expects an array in return:
   *                           ['success' => TRUE/FALSE, 'context' => array of k => v pairs]
   *                           Context data will be added to element in the processing queue (if failed)
   *                           or the processed queue (if successful).
   * @return void
   */
  public function addCallback(string $jobName, callable $callable)
  {
    $this->callbacks[$jobName] = $callable;
  }

  protected function log($level, $message, $data = [])
  protected function log(string $level, string $message, array $data = [])
  {
    if (!isset($this->config['logger'])) {
      return;
    }

    $this->config['logger']->$level($message, $data);
  }
}
