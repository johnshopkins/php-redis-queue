<?php

namespace PhpRedisQueue;

use Psr\Log\LoggerInterface;

class Client
{
  protected $defaultConfig = [
    'logger' => null, // instance of Psr\Log\LoggerInterface
  ];

  protected $config = [];

  /**
   * @param \Predis\Client $redis
   * @param LoggerInterface|null $logger
   */
  public function __construct(protected \Predis\Client $redis, array $config = [])
  {
    $this->config = array_merge($this->defaultConfig, $config);

    if (isset($this->config['logger']) && !$this->config['logger'] instanceof \Psr\Log\LoggerInterface) {
      throw new \InvalidArgumentException('Logger must be an instance of Psr\Log\LoggerInterface.');
    }
  }

  /**
   * Pushes a job to the end of the queue
   * @param string $queue   Queue name (DO NOT prefix with `queue:`). Ex: `akamai_rsync`
   * @param string $jobName Name of the specific job to run, defaults to `default`. Ex: `upload`
   * @param array $data     Data associated with this job
   * @return mixed
   */
  public function push(string $queue, string $jobName = 'default', array $data = [])
  {
    $data = array_merge($data, [
      'queue' => $queue, // used to debug processing and procecssed queues
      'datetime' => $this->getDatetime(),
      'job' => $jobName,
      'id' => $this->getJobId(),
    ]);

    return $this->redis->rpush('queue:' . $queue, json_encode($data));
  }

  protected function getDatetime(): string
  {
    $now = new \DateTime('now', new \DateTimeZone('America/New_York'));
    return $now->format('Y-m-d\TH:i:s');
  }

  protected function getJobId(): int
  {
    return $this->redis->incr('queue:meta:id');
  }

  protected function log($level, $message, $data = [])
  {
    if (!isset($this->config['logger'])) {
      return;
    }

    $this->logger->$level($message, $data);
  }
}
