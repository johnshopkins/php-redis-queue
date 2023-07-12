<?php

namespace PhpRedisQueue;

use Middlewares\TrailingSlash;
use Slim\Factory\AppFactory;
use Slim\Psr7\Request;
use Slim\Psr7\Response;
use Slim\Routing\RouteCollectorProxy;

class Dashboard
{
  /**
   * @param \Predis\Client $redis
   * @param string $baseUrl          Base URL
   */
  public function __construct(protected \Predis\Client $redis, protected string $baseUrl = '/')
  {
    $loader = new \Twig\Loader\FilesystemLoader(__DIR__ . '/views');
    $this->twig = new \Twig\Environment($loader);
  }

  public function render()
  {
    $app = AppFactory::create();

    // Add Error Handling Middleware
    $app->addErrorMiddleware(true, false, false);

    $app->group($this->baseUrl, function (RouteCollectorProxy $group) {

      $group->get('/', function (Request $request, Response $response, $args) {

        $success = array_map('json_decode', $this->redis->lrange('queue:processed:success', 0, -1));
        $failed = array_map('json_decode', $this->redis->lrange('queue:processsed:failed', 0, -1));

        $content = $this->renderTemplate('screens/overview', [
          'baseURL' => $this->baseUrl,
          'success' => $success,
          'failed' => $failed,
        ]);

        $response->getBody()->write($content);

        return $response;
      });

      $group->get('/queues/', function (Request $request, Response $response, $args) {

        $response->getBody()->write('queues');

        return $response;
      });

    });

    $app->add((new TrailingSlash(true))->redirect());

    $app->run();
  }

  protected function renderTemplate(string $template, array $data = [])
  {
    // convert any nested objects to arrays
    $data = json_decode(json_encode($data), true);

    $template = $this->twig->load($template . '.twig');

    return $template->render($data);
  }

  protected function printValue($value)
  {
    if (is_array($value) || is_object($value)) {
      ?>
      <table>
        <?php foreach ((array) $value as $k => $v) : ?>
          <tr><td><?= $k ?></td><td><?php $this->printValue($v) ?></td></tr>
        <?php endforeach; ?>
      </table>
      <?php
      return;
    }

    echo $value;
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
          <td><?php $this->printValue($record->job); ?></td>
        </tr>
      <?php endif; ?>
      <?php if ($record->context && $record->context) : ?>
        <tr>
          <td>Context</td>
          <td><?php $this->printValue($record->context); ?></td>
        </tr>
      <?php endif; ?>
    </table>
    <?php
  }
}
