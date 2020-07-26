#' Clear the Skyscraper Cache
#' @import R.cache
#' @export

clearSkyscraperCache <-
    function() {
        R.cache::clearCache(path = getCachePath("skyscraper"))
    }



#' Cache the Scraped object
#' @import R.cache
#' @param source Source vocabulary such as "CancerGov" or "UpToDate"
#' @export

cacheScrape <-
    function(object,
             page,
             url = NULL,
             source) {

        R.cache::saveCache(object=object,
                           key=list(page,
                                    url,
                                    source),
                           dirs="skyscraper")
    }

#' Load a Cached Scraped Object
#' @import R.cache
#' @export

loadCachedScrape <-
    function(page,
             url,
             source) {

        R.cache::loadCache(key=list(page,
                                    url,
                                    source),
                           dirs = "skyscraper")
    }
