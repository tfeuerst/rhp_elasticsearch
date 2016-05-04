<?php
namespace Rhp\Elasticsearch\Command;


use Elastica\Client;
use Elastica\Document;
use Elastica\Type;
use TYPO3\CMS\Core\Database\DatabaseConnection;
use TYPO3\CMS\Core\Utility\GeneralUtility;
use TYPO3\CMS\Extbase\Mvc\Controller\CommandController;

class ImportCommandController extends CommandController
{

    /**
     * @var \Elastica\Index
     */
    protected $index;

    /**
     * @var \Elastica\Client
     */
    protected $elasticClient;

    /**
     * @var array Typoscript Configuration
     */
    protected $conf;

    /**
     * Imports data from DB to Elasticsearch
     */
    public function importDataCommand()
    {

        $this->connect();
        $this->readNews();
    }

    protected function readNews()
    {
        $res = $this->getDatabaseConnection()->exec_SELECTquery(
            '*',
            'tx_news_domain_model_news',
            'deleted = 0 AND hidden = 0 OR title != "" OR title IS NOT NULL',
            '',
            ''
//           , '0,1'
        );
        while ($row = $this->getDatabaseConnection()->sql_fetch_assoc($res)) {
            $this->outputLine('Found row ' . $row['uid']);
            $toElastic = [
                'id' => $row['uid'],
                'title' => trim($row['title']),
                'teaser' => trim($row['teaser']),
                'bodytext' => trim($row['bodytext']),
                'startTime' => date('Y-m-d H:i', $row['starttime']),
                'endTime' => ($row['endtime'] == '0') ? '2099-12-24 17:00' : date('Y-m-d H:i', $row['endtime']),
                'datetime' => date('d.m.Y', $row['datetime']),
                'artikel_id' => $row['artikel_id'],
                'priority' => (int)$row['priority'],
                'publish' => (bool)$row['publish'],
                'supplier' => $row['supplier'],
                'is_aufmacher' => (bool)$row['is_aufmacher'],
                'is_hp_slider' => (bool)$row['is_hp_slider'],
                'is_pr_article' => (bool)$row['is_pr_article'],
                'keywords' => $this->splitKeywords($row['keywords']),
                'categories' => $this->getCategories($row['uid'])
            ];
            /** @var Document $doc */
            try {
                $doc = GeneralUtility::makeInstance(Document::class, $toElastic['id'], $toElastic);
            } catch (\Exception $e) {
                var_dump($toElastic);
                throw new \Exception('Adding Document to elasticsearch failed.');
            }
            $doc->setType('artikel');
            $doc->setIndex($this->conf['elastic.']['indexName']);
            /** @var Type $type */
            $type = $this->index->getType('artikel');
            $type->addDocument($doc);
        }
    }

    /**
     * Returns an array of set categories
     *
     * @param int $newsUid
     *
     * @return array
     */
    protected function getCategories($newsUid)
    {
        $res = $this->getDatabaseConnection()->exec_SELECTgetRows(
            'c.title AS title',
            'tx_news_domain_model_news_category_mm mm
                INNER JOIN tx_news_domain_model_category c ON c.uid = mm.uid_foreign
                INNER JOIN tx_news_domain_model_news n ON n.uid = mm.uid_local',
            'mm.uid_local = ' . (int)$newsUid . '
            AND n.deleted = 0'
        );
        return $res;
    }

    /**
     * Splits the current keywords into an trimmed array
     *
     * @param string $keywords
     *
     * @return array
     */
    protected function splitKeywords($keywords)
    {
        $keywordList = [];
        if (!empty($keywords)) {
            $keywords = GeneralUtility::trimExplode('/', $keywords, true);
        }
        if ($keywords === 'Keyword1, keyword2, keyword3'
            || !is_array($keywords)
        ) {
            return null;
        }
        foreach ($keywords as $index => $keyword) {
            $keywordList[] = ['title' => $keyword];
        }
        return $keywordList;
    }

    //@todo get TS config from outside
    protected function connect()
    {
        $conf = [
            'host' => 'local.typo3.org',
            'port' => 9200,
            'path' => '',
            'transport' => 'Http',
            'indexName' => 'rheinpfalz'
        ];
        $this->conf = $conf;
        $elasticClientConfiguration = [
            'host' => $conf['host'],
            'port' => $conf['port'],
            'path' => $conf['path'],
            'transport' => $conf['transport']

        ];
        $this->elasticClient = GeneralUtility::makeInstance(Client::class, $elasticClientConfiguration);
        
        $this->index = $this->elasticClient->getIndex($conf['indexName']);
    }

    /**
     * Maps Index
     */
    public function mapCommand()
    {
        $this->connect();
        try {
            $this->index->delete();
        } catch (\Exception $e) {
            $this->outputLine('Deleting the index failed');
        }
        $mapping = [
            'artikel' => [
                'properties' => [
                    'id' => [
                        'type' => 'string',
                        'index' => 'not_analyzed',
                        'include_in_all' => false
                    ],
                    'title' => [
                        'type' => 'string',
                        'index' => 'analyzed',
                        'analyzer' => 'german'
                    ],
                    'teaser' => [
                        'type' => 'string',
                        'index' => 'analyzed',
                        'analyzer' => 'german'
                    ],
                    'bodytext' => [
                        'type' => 'string',
                        'index' => 'analyzed',
                        'analyzer' => 'german'
                    ],
                    'startTime' => [
                        'type' => 'date',
                        'include_in_all' => false,
                        'format' => 'yyyy-MM-dd HH:mm'
                    ],
                    'endTime' => [
                        'type' => 'date',
                        'include_in_all' => false,
                        'format' => 'yyyy-MM-dd HH:mm'
                    ],
                    'datetime' => [
                        'type' => 'date',
                        'include_in_all' => false,
                        'format' => 'dd.mm.yyyy'
                    ],
                    'artikel_id' => [
                        'type' => 'string',
                        'index' => 'not_analyzed',
                    ],
                    'supplier' => [
                        'type' => 'string',
                        'index' => 'not_analyzed',
                    ],
                    'keywords' => [
                        'type' => 'object',
                        'properties' => [
                            'title' => [
                                'type' => 'string',
                                'index' => 'not_analyzed',
                            ]
                        ]
                    ],
                    'categories' => [
                        'type' => 'object',
                        'properties' => [
                            'title' => [
                                'type' => 'string',
                                'index' => 'not_analyzed',
                            ]
                        ]
                    ]
                ]
            ]
        ];
        $this->index->create(
            [
                'mappings' => $mapping
            ]
        );
        $this->outputLine('Mapping done');
    }

    /**
     * @return DatabaseConnection
     */
    protected function getDatabaseConnection()
    {
        return $GLOBALS['TYPO3_DB'];
    }
}
