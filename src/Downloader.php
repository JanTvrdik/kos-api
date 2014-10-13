<?php
namespace JanTvrdik\KosApi;

use DateTime;
use Kdyby\CurlCaBundle;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use RollingCurl\Request;
use RollingCurl\RollingCurl;
use SimpleXMLElement;


class Downloader implements LoggerAwareInterface
{
	/** @const int maximum allowed limit by KOS API, see https://kosapi.fit.cvut.cz/projects/kosapi/wiki/URLParameters#limit */
	const MAX_LIMIT = 1000;

	/** @const int */
	const MAX_RETRIES = 3;

	/** @const string base API URL */
	const API_URL = 'https://kosapi.fit.cvut.cz/api/3/';

	/** @var string */
	private $username;

	/** @var string */
	private $password;

	/** @var int maximum concurrent connections */
	private $maxConnections;

	/** @var string */
	private $currentSemester;

	/** @var string|null */
	private $cacheDir;

	/** @var array (resource => page) */
	private $currentPages = [];

	/** @var array (urlHash => retry counter) */
	private $retriesCounter = [];

	/** @var RollingCurl */
	private $curl;

	/** @var LoggerInterface */
	private $logger;

	/**
	 * Constructor.
	 */
	public function __construct(array $config)
	{
		$this->username = $config['user'];
		$this->password = $config['password'];
		$this->maxConnections = $config['max_connections'];
		$this->currentSemester = $config['semester'];
		$this->cacheDir = isset($config['cache_dir']) ? $config['cache_dir'] : NULL;
		$this->logger = new NullLogger();
		$this->curl = $this->createCurl();

		if ($this->cacheDir && !is_dir($this->cacheDir)) {
			$this->logger->notice(sprintf('Directory "%s" does not exist -> creating.', $this->cacheDir));
			@mkdir($this->cacheDir);
			if (!is_dir($this->cacheDir)) {
				$this->logger->warning(sprintf('Failed to create directory "%s" -> ignoring.', $this->cacheDir));
				$this->cacheDir = NULL;
			}
		}
	}

	/**
	 * Sets a logger instance on the object
	 *
	 * @param LoggerInterface $logger
	 * @return null
	 */
	public function setLogger(LoggerInterface $logger)
	{
		$this->logger = $logger;
	}

	/**
	 * Starts downloading and processing scheduled requests.
	 *
	 * @return void
	 */
	public function startDownload()
	{
		while ($this->curl->countPending()) {
			$this->curl->execute();
		}
	}

	/**
	 * Schedules request for processing.
	 *
	 * @param array $request
	 * @return void
	 */
	public function addRequest(array $request)
	{
		$resource = $request['resource'];
		$params = isset($request['params']) ? $request['params'] : [];
		$extra = isset($request['extra']) ? $request['extra'] : [];
		$limit = isset($params['limit']) ? $params['limit'] : self::MAX_LIMIT;
		$page = isset($this->currentPages[$resource]) ? (++$this->currentPages[$resource]) : ($this->currentPages[$resource] = 0);

		$params += [
			'offset' => $page * $limit,
			'limit' => $limit,
			'sem' => $this->currentSemester,
			'lang' => 'cs',
			'multilang' => 'false',
		];

		$extra += [
			'page' => $page,
			'callback' => function (Request $curlRequest) use ($request) {
				$decodedUrl = urldecode($curlRequest->getUrl());
				$this->logger->debug('Request completed:      ' . $decodedUrl);
				$responseInfo = $curlRequest->getResponseInfo();
				if ($curlRequest->getResponseErrno() !== 0) {
					throw new CurlException('cUrl error: ' . $curlRequest->getResponseError());
				}
				if ($responseInfo['http_code'] !== 200) {
					if ($this->shouldRetry($decodedUrl)) {
						$this->logger->warning(sprintf('HTTP error #%d: %s -> retrying', $responseInfo['http_code'], $decodedUrl));
						$this->addRequest($request);

						return;

					} else {
						$this->logger->warning(sprintf('HTTP error #%d: %s -> ignoring', $responseInfo['http_code'], $decodedUrl));

						return;
					}
				}

				$responseText = $curlRequest->getResponseText();
				$xml = @simplexml_load_string($responseText);
				if ($xml === FALSE) {
					if ($this->shouldRetry($decodedUrl)) {
						$this->logger->warning(sprintf('Received invalid XML: %s -> retrying', $decodedUrl));
						$this->addRequest($request);

						return;

					} else {
						$this->logger->error(sprintf('Received invalid XML: %s -> terminating', $decodedUrl));
						throw new InvalidXmlException();
					}
				}

				if ($this->cacheDir) {
					$cacheFile = $this->cacheDir . '/' . md5($curlRequest->getUrl()) . '.xml';
					file_put_contents($cacheFile, $responseText, LOCK_EX);
				}

				if ($this->hasNext($xml)) {
					$this->addRequest($request);
				}
				$request['callback']($xml, $request);
			},
		];

		$url = self::API_URL . $resource . '?' . http_build_query($params);

		if ($this->cacheDir) {
			$cacheFile = $this->cacheDir . '/' . md5($url) . '.xml';
			if (is_file($cacheFile)) {
				$xml = @simplexml_load_file($cacheFile);
				if ($this->hasNext($xml)) {
					$this->addRequest($request);
				}
				$request['callback']($xml, $request);

				return;
			}
		}

		$curlRequest = new Request($url);
		$curlRequest->setExtraInfo($extra);
		$this->curl->add($curlRequest);

		$this->logger->debug('Scheduling to download: ' . urldecode($curlRequest->getUrl()));
	}

	/**
	 * Checks whether another page follows the current.
	 *
	 * @param SimpleXMLElement $xml root <atom:feed> element
	 * @return bool
	 */
	public function hasNext(SimpleXMLElement $xml)
	{
		return (bool) $xml->xpath('/atom:feed/atom:link[@rel=\'next\']');
	}

	/**
	 * Returns numeric ID from <atom:id> element which is direct descendant of <atom:entry> element.
	 *
	 * @param SimpleXMLElement $entry <atom:entry> element
	 * @return int
	 */
	public function getId(SimpleXMLElement $entry)
	{
		$tmp = $entry->xpath('atom:id');
		$id = substr($tmp[0], strrpos($tmp[0], ':') + 1);

		return (int) $id;
	}

	/**
	 * Returns date and time when given <atom:entry> element was last time updated.
	 *
	 * @param SimpleXMLElement $entry <atom:entry> element
	 * @return DateTime
	 */
	public function getUpdated(SimpleXMLElement $entry)
	{
		$tmp = $entry->xpath('atom:updated');
		$updated = new DateTime($tmp[0]);

		return $updated;
	}

	/**
	 * Returns resource code or id from element pointing to them with 'xlink:href' attribute,
	 * e.g. <course xlink:href="courses/BI-LIN/">BI-LIN</course>.
	 *
	 * @param SimpleXMLElement $el
	 * @return string
	 */
	public function getResourceCode(SimpleXMLElement $el)
	{
		$href = $this->getHrefAttr($el);
		$href = trim($href, '/');
		$code = substr($href, strrpos($href, '/') + 1);

		return $code;
	}

	/**
	 * Returns content of 'xlink:href' attribute.
	 *
	 * @param SimpleXMLElement $el
	 * @return string
	 */
	public function getHrefAttr(SimpleXMLElement $el)
	{
		return (string) $el->attributes('http://www.w3.org/1999/xlink')->href;
	}

	/**
	 * Checks whether requesting given URL should be tried one more time.
	 *
	 * @param string $url
	 * @return bool
	 */
	private function shouldRetry($url)
	{
		if (!isset($this->retriesCounter[$url])) {
			$this->retriesCounter[$url] = 1;
		} else {
			$this->retriesCounter[$url]++;
		}

		return ($this->retriesCounter[$url] < self::MAX_RETRIES);
	}

	/**
	 * Creates and configures RollingCurl instance.
	 *
	 * @return RollingCurl
	 */
	private function createCurl()
	{
		$curl = new RollingCurl();

		$curl->addOptions([
			CURLOPT_CAINFO => CurlCaBundle\CertificateHelper::getCaInfoFile(),
			CURLOPT_TIMEOUT => 300,
		]);

		$curl->setHeaders([
			'Accept: application/atom+xml',
			'Authorization: Basic ' . base64_encode($this->username . ':' . $this->password),
		]);

		$curl->setSimultaneousLimit($this->maxConnections);

		$curl->setCallback(function (Request $request, RollingCurl $curl) {
			$extra = $request->getExtraInfo();
			$callback = $extra['callback'];
			$callback($request, $curl);
		});

		return $curl;
	}
}
