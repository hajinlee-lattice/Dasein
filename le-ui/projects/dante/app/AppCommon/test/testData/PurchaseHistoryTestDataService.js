angular.module('test.testData.PurchaseHistoryTestDataService', [
])
.service('PurchaseHistoryTestDataService', function () {

    // type: account or segment
    this.GetSampleAccount = function (type) {
        switch (type) {
            case 'segment':
                return {
                    "BaseExternalID":"SAMPLE_SEGMENT",
                    "NotionName":"DanteAccount",
                    "DisplayName":"Sample Segment",
                    "IsSegment":true
                };
            case 'account':
            default:
                return {
                    "BaseExternalID":"SAMPLE_ACCOUNT",
                    "NotionName":"DanteAccount",
                    "DisplayName":"Sample Account",
                    "IsSegment":false
                };
        }
    };

    this.GetSampleProductHierarchies = function () {
        return [{
            "Level1Name": "Finance",
            "Level2Name": "Services",
            "Level3Name": "Consulting",
            "ProductHierarchyID": "PHch000"
        },{
            "Level1Name": "Finance",
            "Level2Name": "Services",
            "Level3Name": "Management",
            "ProductHierarchyID": "PHch001"
        }];
    };

    this.GetSampleProductHierarchiesMapped = function () {
        return {
            "PHch000": {
                "Level1Name": "Finance",
                "Level2Name": "Services",
                "Level3Name": "Consulting",
                "ProductHierarchyID": "PHch000"
            },
            "PHch001": {
                "Level1Name": "Finance",
                "Level2Name": "Services",
                "Level3Name": "Management",
                "ProductHierarchyID": "PHch001"
            }
        };
    };

    this.GetSampleDailyPurchaseHistory = function () {
        return {
            "Period": 'D',
            "PeriodStartDate": '1462150050',
            "PurchaseHistoryAttributes": [{
                "PeriodOffset": 1,
                "ProductHierarchyID": 'PHch000',
                "TotalSpend": 100,
                "TotalVolume": 4,
                "TransactionCount": 3
            },{
                "PeriodOffset": 2,
                "ProductHierarchyID": 'PHch000',
                "TotalSpend": 90,
                "TotalVolume": 5,
                "TransactionCount": 2
            },{
                "PeriodOffset": 35,
                "ProductHierarchyID": 'PHch000',
                "TotalSpend": 60,
                "TotalVolume": 1,
                "TransactionCount": 1
            },{
                "PeriodOffset": 100,
                "ProductHierarchyID": 'PHch000',
                "TotalSpend": 150,
                "TotalVolume": 9,
                "TransactionCount": 4
            },{
                "PeriodOffset": 101,
                "ProductHierarchyID": 'PHch000',
                "TotalSpend": 180,
                "TotalVolume": 6,
                "TransactionCount": 5
            },{
                "PeriodOffset": 1,
                "ProductHierarchyID": 'PHch001',
                "TotalSpend": 80,
                "TotalVolume": 5,
                "TransactionCount": 2
            },{
                "PeriodOffset": 2,
                "ProductHierarchyID": 'PHch001',
                "TotalSpend": 60,
                "TotalVolume": 3,
                "TransactionCount": 3
            },{
                "PeriodOffset": 35,
                "ProductHierarchyID": 'PHch001',
                "TotalSpend": 90,
                "TotalVolume": 4,
                "TransactionCount": 1
            },{
                "PeriodOffset": 100,
                "ProductHierarchyID": 'PHch001',
                "TotalSpend": 120,
                "TotalVolume": 3,
                "TransactionCount": 2
            },{
                "PeriodOffset": 101,
                "ProductHierarchyID": 'PHch001',
                "TotalSpend": 200,
                "TotalVolume": 10,
                "TransactionCount": 4
            }]
        };
    };

    this.GetSampleMonthlyPurchaseHistory = function () {
        return {
            "Period": 'M',
            "PeriodStartDate": '1462150050',
            "PurchaseHistoryAttributes": [{
                "PeriodOffset": 0,
                "ProductHierarchyID": 'PHch000',
                "TotalSpend": 100,
                "TotalVolume": 4,
                "TransactionCount": 3
            },{
                "PeriodOffset": 1,
                "ProductHierarchyID": 'PHch000',
                "TotalSpend": 90,
                "TotalVolume": 5,
                "TransactionCount": 2
            },{
                "PeriodOffset": 15,
                "ProductHierarchyID": 'PHch000',
                "TotalSpend": 150,
                "TotalVolume": 9,
                "TransactionCount": 4
            },{
                "PeriodOffset": 16,
                "ProductHierarchyID": 'PHch000',
                "TotalSpend": 180,
                "TotalVolume": 6,
                "TransactionCount": 5
            },{
                "PeriodOffset": 0,
                "ProductHierarchyID": 'PHch001',
                "TotalSpend": 80,
                "TotalVolume": 5,
                "TransactionCount": 2
            },{
                "PeriodOffset": 1,
                "ProductHierarchyID": 'PHch001',
                "TotalSpend": 60,
                "TotalVolume": 3,
                "TransactionCount": 3
            },{
                "PeriodOffset": 15,
                "ProductHierarchyID": 'PHch001',
                "TotalSpend": 120,
                "TotalVolume": 3,
                "TransactionCount": 2
            },{
                "PeriodOffset": 16,
                "ProductHierarchyID": 'PHch001',
                "TotalSpend": 200,
                "TotalVolume": 10,
                "TransactionCount": 4
            }]
        };
    };

    this.GetSampleProductTreeFromDaily = function () {
        return {
            "displayName": "root",
            //"lastTransactionDate": "2016 Aug",
            //"periodStartDate": "2016 May",
            "data": {
                "M": {
                    "2016 May": {
                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 330, "TotalVolume": 17, "TransactionCount": 10, "AverageSpend": 330 }
                    },
                    "2016 Jun": {
                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 150, "TotalVolume": 5, "TransactionCount": 2, "AverageSpend": 150 }
                    },
                    "2016 Aug": {
                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 650, "TotalVolume": 28, "TransactionCount": 15, "AverageSpend": 650 }
                    }
                },
                "Q": {
                    "2016 2": {
                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 480, "TotalVolume": 22, "TransactionCount": 12, "AverageSpend": 480 }
                    },
                    "2016 3": {
                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 650, "TotalVolume": 28, "TransactionCount": 15, "AverageSpend": 650 }
                    }
                }
            },
            "nodes": {
                "Finance": {
                    "displayName": "Finance",
                    "data": {
                        "M": {
                            "2016 May": {
                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 330, "TotalVolume": 17, "TransactionCount": 10, "AverageSpend": 330 }
                            },
                            "2016 Jun": {
                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 150, "TotalVolume": 5, "TransactionCount": 2, "AverageSpend": 150 }
                            },
                            "2016 Aug": {
                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 650, "TotalVolume": 28, "TransactionCount": 15, "AverageSpend": 650 }
                            }
                        },
                        "Q": {
                            "2016 2": {
                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 480, "TotalVolume": 22, "TransactionCount": 12, "AverageSpend": 480 }
                            },
                            "2016 3": {
                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 650, "TotalVolume": 28, "TransactionCount": 15, "AverageSpend": 650 }
                            }
                        }
                    },
                    "nodes": {
                        "Services": {
                            "displayName": "Services",
                            "data": {
                                "M": {
                                    "2016 May": {
                                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 330, "TotalVolume": 17, "TransactionCount": 10, "AverageSpend": 330 }
                                    },
                                    "2016 Jun": {
                                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 150, "TotalVolume": 5, "TransactionCount": 2, "AverageSpend": 150 }
                                    },
                                    "2016 Aug": {
                                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 650, "TotalVolume": 28, "TransactionCount": 15, "AverageSpend": 650 }
                                    }
                                },
                                "Q": {
                                    "2016 2": {
                                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 480, "TotalVolume": 22, "TransactionCount": 12, "AverageSpend": 480 }
                                    },
                                    "2016 3": {
                                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 650, "TotalVolume": 28, "TransactionCount": 15, "AverageSpend": 650 }
                                    }
                                }
                            },
                            "nodes": {
                                "Consulting": {
                                    "displayName": "Consulting",
                                    "data": {
                                        "M": {
                                            "2016 May": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 190, "TotalVolume": 9, "TransactionCount": 5, "AverageSpend": 190 }
                                            },
                                            "2016 Jun": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 60, "TotalVolume": 1, "TransactionCount": 1, "AverageSpend": 60 }
                                            },
                                            "2016 Aug": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 330, "TotalVolume": 15, "TransactionCount": 9, "AverageSpend": 330 }
                                            }
                                        },
                                        "Q": {
                                            "2016 2": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 250, "TotalVolume": 10, "TransactionCount": 6, "AverageSpend": 250 }
                                            },
                                            "2016 3": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 330, "TotalVolume": 15, "TransactionCount": 9, "AverageSpend": 330 }
                                            }
                                        }
                                    },
                                    "nodes": {}
                                },
                                "Management": {
                                    "displayName": "Management",
                                    "data": {
                                        "M": {
                                            "2016 May": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 140, "TotalVolume": 8, "TransactionCount": 5, "AverageSpend": 140 }
                                            },
                                            "2016 Jun": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 90, "TotalVolume": 4, "TransactionCount": 1, "AverageSpend": 90 }
                                            },
                                            "2016 Aug": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 320, "TotalVolume": 13, "TransactionCount": 6, "AverageSpend": 320 }
                                            }
                                        },
                                        "Q": {
                                            "2016 2": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 230, "TotalVolume": 12, "TransactionCount": 6, "AverageSpend": 230 }
                                            },
                                            "2016 3": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 320, "TotalVolume": 13, "TransactionCount": 6, "AverageSpend": 320 }
                                            }
                                        }
                                    },
                                    "nodes": {}
                                }
                            }
                        }
                    }
                }
            }
        };
    };

    this.GetSampleProductTreeFromMonthly = function () {
        return {
            "displayName": "root",
            //"lastTransactionDate": "2017 Sep",
            //"periodStartDate": "2016 May",
            "data": {
                "M": {
                    "2016 May": {
                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 180, "TotalVolume": 9, "TransactionCount": 5, "AverageSpend": 180 }
                    },
                    "2016 Jun": {
                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 150, "TotalVolume": 8, "TransactionCount": 5, "AverageSpend": 150 }
                    },
                    "2017 Aug": {
                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 270, "TotalVolume": 12, "TransactionCount": 6, "AverageSpend": 270 }
                    },
                    "2017 Sep": {
                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 380, "TotalVolume": 16, "TransactionCount": 9, "AverageSpend": 380 }
                    }
                },
                "Q": {
                    "2016 2": {
                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 330,"TotalVolume": 17, "TransactionCount": 10, "AverageSpend": 330 }
                    },
                    "2017 3": {
                      "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 650,"TotalVolume": 28, "TransactionCount": 15, "AverageSpend": 650 }
                    }
                }
            },
            "nodes": {
                "Finance": {
                    "displayName": "Finance",
                    "data": {
                        "M": {
                            "2016 May": {
                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 180, "TotalVolume": 9, "TransactionCount": 5, "AverageSpend": 180 }
                            },
                            "2016 Jun": {
                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 150, "TotalVolume": 8, "TransactionCount": 5, "AverageSpend": 150 }
                            },
                            "2017 Aug": {
                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 270, "TotalVolume": 12, "TransactionCount": 6, "AverageSpend": 270 }
                            },
                            "2017 Sep": {
                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 380, "TotalVolume": 16, "TransactionCount": 9, "AverageSpend": 380 }
                            }
                        },
                        "Q": {
                            "2016 2": {
                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 330,"TotalVolume": 17, "TransactionCount": 10, "AverageSpend": 330 }
                            },
                            "2017 3": {
                              "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 650,"TotalVolume": 28, "TransactionCount": 15, "AverageSpend": 650 }
                            }
                        }
                    },
                    "nodes": {
                        "Services": {
                            "displayName": "Services",
                            "data": {
                                "M": {
                                    "2016 May": {
                                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 180, "TotalVolume": 9, "TransactionCount": 5, "AverageSpend": 180 }
                                    },
                                    "2016 Jun": {
                                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 150, "TotalVolume": 8, "TransactionCount": 5, "AverageSpend": 150 }
                                    },
                                    "2017 Aug": {
                                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 270, "TotalVolume": 12, "TransactionCount": 6, "AverageSpend": 270 }
                                    },
                                    "2017 Sep": {
                                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 380, "TotalVolume": 16, "TransactionCount": 9, "AverageSpend": 380 }
                                    }
                                },
                                "Q": {
                                    "2016 2": {
                                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 330,"TotalVolume": 17, "TransactionCount": 10, "AverageSpend": 330 }
                                    },
                                    "2017 3": {
                                        "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 650,"TotalVolume": 28, "TransactionCount": 15, "AverageSpend": 650 }
                                    }
                                }
                            },
                            "nodes": {
                                "Consulting": {
                                    "displayName": "Consulting",
                                    "data": {
                                        "M": {
                                            "2016 May": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 100,"TotalVolume": 4,"TransactionCount": 3,"AverageSpend": 100 }
                                            },
                                            "2016 Jun": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 90,"TotalVolume": 5,"TransactionCount": 2,"AverageSpend": 90 }
                                            },
                                            "2017 Aug": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 150,"TotalVolume": 9,"TransactionCount": 4,"AverageSpend": 150 }
                                            },
                                            "2017 Sep": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 180,"TotalVolume": 6,"TransactionCount": 5,"AverageSpend": 180 }
                                            }
                                        },
                                        "Q": {
                                            "2016 2": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 190,"TotalVolume": 9, "TransactionCount": 5,"AverageSpend": 190 }
                                            },
                                            "2017 3": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 330,"TotalVolume": 15, "TransactionCount": 9,"AverageSpend": 330 }
                                            }
                                        }
                                    },
                                    "nodes": {}
                                },
                                "Management": {
                                    "displayName": "Management",
                                    "data": {
                                        "M": {
                                            "2016 May": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 80, "TotalVolume": 5, "TransactionCount": 2,"AverageSpend": 80 }
                                            },
                                            "2016 Jun": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 60, "TotalVolume": 3, "TransactionCount": 3,"AverageSpend": 60 }
                                            },
                                            "2017 Aug": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 120, "TotalVolume": 3, "TransactionCount": 2,"AverageSpend": 120 }
                                            },
                                            "2017 Sep": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 200, "TotalVolume": 10, "TransactionCount": 4,"AverageSpend": 200 }
                                            }
                                        },
                                        "Q": {
                                            "2016 2": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 140,"TotalVolume": 8,"TransactionCount": 5,"AverageSpend": 140 }
                                            },
                                            "2017 3": {
                                              "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 320,"TotalVolume": 13,"TransactionCount": 6,"AverageSpend": 320 }
                                            }
                                        }
                                    },
                                    "nodes": {}
                                }
                            }
                        }
                    }
                }
            }
        };
    };

    // first step of PurchaseHistoryService.constructPurchaseHistory
    // gets passed into PurchaseHistoryService.depthFirstAggregate
    // only have data at level 3
    this.GetSampleProductTreeFromDailyAtLevel3 = function () {
        return {
            "displayName": "root",
            //"lastTransactionDate": "2016 Aug",
            //"periodStartDate": "2016 May",
            "data": {
                "M": {},
                "Q": {}
            },
            "nodes": {
                "Finance": {
                    "displayName": "Finance",
                    "data": {
                        "M": {},
                        "Q": {}
                    },
                    "nodes": {
                        "Services": {
                            "displayName": "Services",
                            "data": {
                                "M": {},
                                "Q": {}
                            },
                            "nodes": {
                                "Consulting": {
                                    "displayName": "Consulting",
                                    "data": {
                                        "M": {
                                            "2016 May": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 190, "TotalVolume": 9, "TransactionCount": 5, "AverageSpend": 190 }
                                            },
                                            "2016 Jun": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 60, "TotalVolume": 1, "TransactionCount": 1, "AverageSpend": 60 }
                                            },
                                            "2016 Aug": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 330, "TotalVolume": 15, "TransactionCount": 9, "AverageSpend": 330 }
                                            }
                                        },
                                        "Q": {
                                            "2016 2": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 250, "TotalVolume": 10, "TransactionCount": 6, "AverageSpend": 250 }
                                            },
                                            "2016 3": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 330, "TotalVolume": 15, "TransactionCount": 9, "AverageSpend": 330 }
                                            }
                                        }
                                    },
                                    "nodes": {}
                                },
                                "Management": {
                                    "displayName": "Management",
                                    "data": {
                                        "M": {
                                            "2016 May": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 140, "TotalVolume": 8, "TransactionCount": 5, "AverageSpend": 140 }
                                            },
                                            "2016 Jun": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 90, "TotalVolume": 4, "TransactionCount": 1, "AverageSpend": 90 }
                                            },
                                            "2016 Aug": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 320, "TotalVolume": 13, "TransactionCount": 6, "AverageSpend": 320 }
                                            }
                                        },
                                        "Q": {
                                            "2016 2": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 230, "TotalVolume": 12, "TransactionCount": 6, "AverageSpend": 230 }
                                            },
                                            "2016 3": {
                                                "SAMPLE_ACCOUNT": { "RepresentativeAccounts":1, "TotalSpend": 320, "TotalVolume": 13, "TransactionCount": 6, "AverageSpend": 320 }
                                            }
                                        }
                                    },
                                    "nodes": {}
                                }
                            }
                        }
                    }
                }
            }
        };
    };
});
