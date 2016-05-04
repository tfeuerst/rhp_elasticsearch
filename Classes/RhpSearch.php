<?php
namespace Rhp\Elasticsearch;

/*
 * This file is part of the TYPO3 CMS project.
 *
 * It is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, either version 2
 * of the License, or any later version.
 *
 * For the full copyright and license information, please read the
 * LICENSE.txt file that was distributed with this source code.
 *
 * The TYPO3 project - inspiring people to share!
 */

use Elastica\Client;
use Elastica\Document;
use Elastica\Suggest;
use Elastica\Type;
use TYPO3\CMS\Core\Utility\GeneralUtility;
use TYPO3\CMS\Fluid\View\StandaloneView;

class RhpSearch
{
    /**
     * The Elasticsearch Index Object from Elastica
     *
     * @var \Elastica\Index
     */
    protected $index;

    /**
     * @var StandaloneView
     */
    protected $view;

    /**
     * @var array Typoscript config
     */
    protected $conf;

    public function __construct()
    {
        $this->view = GeneralUtility::makeInstance(StandaloneView::class);
//
        $this->view->setLayoutRootPaths([PATH_site . 'typo3conf/ext/rhp_elasticsearch/Resources/Private/Layouts']);
        $this->view->setTemplateRootPaths([PATH_site . 'typo3conf/ext/rhp_elasticsearch/Resources/Private/Templates']);
        $this->view->setPartialRootPaths([PATH_site . 'typo3conf/ext/rhp_elasticsearch/Resources/Private/Partials']);

    }

    public function main($content, $conf)
    {
        $this->connect($conf);
        $action = 'list';
        $actionParam = GeneralUtility::_GP('action');
        if (!empty($actionParam)) {
            $action = $actionParam;
        }
        // Set template
        $this->view->setTemplatePathAndFilename(PATH_site . 'typo3conf/ext/rhp_elasticsearch/Resources/Private/Templates/'.ucfirst($action).'.html');
        switch($action) {
            case 'search':
                $this->searchView();
                break;
            default:
                $this->listView();
        }
        return $this->view->render();
    }

    protected function searchView()
    {
        $query = GeneralUtility::_GP('query');
        $queryArray = [
            'query' => [
                'bool' => [
                    'must' => [
                        [
                            'query_string' => [
                                'query' => $this->escape($query)
                            ]
                        ]
                    ],
//                    'must_not' => [
//                        [
//                            'terms' => [
//                                'status.name' => [
//                                    'Closed',
//                                    'Rejected',
//                                    'Resolved'
//                                ]
//                            ],
//                        ]
//                    ]
                ],
            ],
            'aggregations' => [
                'category' => [
                    'terms' => [
                        'field' => 'categories.title'
                    ]
                ],
                'keywords' => [
                    'terms' => [
                        'field' => 'keywords.title'
                    ]
                ]
            ],
            'size' => 5,
        ];
        $search = $this->index->createSearch($queryArray);
        // Suggester mit namen "Vorschlag" auf Feld "title" erzeugen
        $termSuggest = new Suggest\Term('vorschlag', '_all');
        $termSuggest->setSuggestMode('popular');
        // Neuen Suggest Block anlegen
        $suggester = new Suggest();
        $suggester->setGlobalText($query);
        $suggester->addSuggestion($termSuggest);
        // Suggestions in Query einbinden
        $suggestions = $this->index->createSearch('');
        $suggestions->setSuggest($suggester);
        $suggestResults = $suggestions->search();

        /**
         * Highlighting
         */
//        $search->getQuery()->addHighlight(array());

        /**
         * SEND SEARCH
         */
        $resultSet = $search->search();
        $results = $resultSet->getResults();
        foreach ($results as $index => $result) {
            $hits[] = $result->getData();
        }
        $aggregations = $resultSet->getAggregations();
        $this->view->assign('results', $hits);
        $this->view->assign('aggs', $aggregations);
        $this->view->assign('suggest', $suggestResults->getSuggests());
//        $this->view->assign('highlights', $resultSet->get)
        $this->view->assign('query', htmlspecialchars($query));
    }

    protected function listView()
    {
        $hits = [];
        $queryArray = [
            'query' => [
                'filtered' => [
                    'query' => [
                        'bool' => [
                            'must' => [
                                [
                                    'terms' => [
                                        'categories.title' => [
                                            'Sport'
                                        ]
                                    ]
                                ]
                            ],
                            'must_not' => [
                                [
                                    'terms' => [
                                        'publish' => [
                                            true
                                        ]
                                    ],
                                ]
                            ]
                        ],
                    ]

                ]
            ],
            'size' => 7,
            'sort' => [
                'datetime' => [
                    'order' => 'desc'
                ],
                'priority' => [
                    'order' => 'desc'
                ],
                'id' => [
                    'order' => 'desc'
                ]
            ]
        ];
        $search = $this->index->createSearch($queryArray);
        $results = $search->search();
        $result = $results->getResults();
        $queryTime = $results->getTotalTime();
        foreach ($result as $index => $item) {
            $hits[] = $item->getData();
        }
        $this->view->assign('time', $queryTime);
        $this->view->assign('list', $hits);

    }

    protected function connect($conf)
    {
        $this->conf = $conf;
        $elasticClientConfiguration = [
            'host' => $conf['elastic.']['host'],
            'port' => $conf['elastic.']['port'],
            'path' => $conf['elastic.']['path'],
            'transport' => $conf['elastic.']['transport']
        ];
        $elasticaClient = GeneralUtility::makeInstance(Client::class, $elasticClientConfiguration);
        $this->index = $elasticaClient->getIndex($conf['elastic.']['indexName']);
    }

    /**
     * @param array $stub Stubbed data to import
     */
    protected function writeStub(array $stub)
    {
        if (empty($stub)) {
            $stub = [
                'id' => '91-48262968',
                'title' => 'Schlägerei im türkischen Parlament',
                'subtitle' => 'Lorem Ipsum dolor amet',
                'author' => [
                    'Hermann Frau',
                    'Frank Schmitz'
                ]
            ];
        }

        /** @var Document $doc */
        $doc = GeneralUtility::makeInstance(Document::class, $stub['id'], $stub);
        $doc->setType('artikel');
        $doc->setIndex($this->conf['elastic.']['indexName']);
        /** @var Type $type */
        $type = $this->index->getType('artikel');
        $type->addDocument($doc);

    }

    /**
     * Escape a value for special query characters such as ':', '(', ')', '*', '?', etc.
     * NOTE: inside a phrase fewer characters need escaped, use {@link Apache_Solr_Service::escapePhrase()} instead
     *
     * @param string $value
     *
     * @return string
     */
    protected function escape($value)
    {
        //list taken from http://lucene.apache.org/java/docs/queryparsersyntax.html#Escaping%20Special%20Characters
        $pattern = '/(\+|-|&&|\|\||!|\(|\)|\{|}|\[|]|\^|"|~|\*|\?|:|\\\)/';
        $replace = '\\\$1';
        $escapedString = preg_replace($pattern, $replace, $value);
        $escapedString = str_replace('/', '\/', $escapedString);

        return $escapedString;
    }
}