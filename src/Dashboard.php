<?php

namespace PhpRedisQueue;

class Dashboard
{
  /**
   * @param \Predis\Client $redis
   */
  public function __construct(protected \Predis\Client $redis)
  {

  }

  public function output()
  {
    $success = array_map('json_decode', $this->redis->lrange('queue:processed:success', 0, -1));
    $failed = array_map('json_decode', $this->redis->lrange('queue:processsed:failed', 0, -1));

    ?>
    <table width="100%" border="1" cellspacing="0" cellpadding="5">
      <tr>
        <th width="50%">Success</th>
        <th width="50%">Fail</th>
      </tr>
      <tr>
        <td valign="top">
          <?php foreach ($success as $record) : ?>
            <?php $this->printRecord($record); ?>
          <?php endforeach; ?>
        </td>
        <td valign="top">
          <?php foreach ($failed as $record) : ?>
            <?php $this->printRecord($record); ?>
          <?php endforeach; ?>
        </td>
      </tr>
    </table>
    <?php
  }

  protected function printRecord($record)
  {
    ?>
    <table border="1" cellspacing="0" cellpadding="5">
      <tr>
        <td>Queue</td>
        <td><?= $record->meta->queue ?></td>
      </tr>
      <tr>
        <td>Job</td>
        <td><?= $record->meta->jobName ?></td>
      </tr>
      <tr>
        <td>ID</td>
        <td><?= $record->meta->id ?></td>
      </tr>
      <tr>
        <td>Datetime</td>
        <td><?= $record->meta->datetime ?></td>
      </tr>
      <?php if ($record->job) : ?>
        <tr>
          <td>Job data</td>
          <td>
            <table>
              <?php foreach ((array) $record->job as $k => $v) : ?>
                <tr><td><?= $k ?></td><td><?= $v ?></td></tr>
              <?php endforeach; ?>
            </table>
          </td>
        </tr>
      <?php endif; ?>
      <?php if ($record->context && $record->context) : ?>
        <tr>
          <td>Context</td>
          <td>
            <table>
              <?php foreach ((array) $record->context as $k => $v) : ?>
                <tr><td><?= $k ?></td><td><?= $v ?></td></tr>
              <?php endforeach; ?>
            </table>
          </td>
        </tr>
      <?php endif; ?>
    </table>
    <?php
  }
}
