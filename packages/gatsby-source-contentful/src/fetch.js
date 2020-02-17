const contentful = require(`contentful`)
const _ = require(`lodash`)
const chalk = require(`chalk`)
const resolveResponse = require(`contentful-resolve-response`)

const normalize = require(`./normalize`)
const { formatPluginOptionsForCLI } = require(`./plugin-options`)

module.exports = async ({ syncToken, reporter, pluginConfig }) => {
  // Fetch articles.
  console.time(`Fetch Contentful data`)

  console.log(`Starting to fetch data from Contentful`)

  const contentfulClientOptions = {
    space: pluginConfig.get(`spaceId`),
    accessToken: pluginConfig.get(`accessToken`),
    host: pluginConfig.get(`host`),
    environment: pluginConfig.get(`environment`),
    // TODO: Remove this once we're using the Sync API again.
    resolveLinks: false,
  }

  const client = contentful.createClient(contentfulClientOptions)

  // The sync API puts the locale in all fields in this format { fieldName:
  // {'locale': value} } so we need to get the space and its default local.
  //
  // We'll extend this soon to support multiple locales.
  let locales
  let defaultLocale = `en-US`
  try {
    console.log(`Fetching default locale`)
    locales = await client.getLocales().then(response => response.items)
    defaultLocale = _.find(locales, { default: true }).code
    locales = locales.filter(pluginConfig.get(`localeFilter`))
    console.log(`default locale is : ${defaultLocale}`)
  } catch (e) {
    let details
    let errors
    if (e.code === `ENOTFOUND`) {
      details = `You seem to be offline`
    } else if (e.response) {
      if (e.response.status === 404) {
        // host and space used to generate url
        details = `Endpoint not found. Check if ${chalk.yellow(
          `host`
        )} and ${chalk.yellow(`spaceId`)} settings are correct`
        errors = {
          host: `Check if setting is correct`,
          spaceId: `Check if setting is correct`,
        }
      } else if (e.response.status === 401) {
        // authorization error
        details = `Authorization error. Check if ${chalk.yellow(
          `accessToken`
        )} and ${chalk.yellow(`environment`)} are correct`
        errors = {
          accessToken: `Check if setting is correct`,
          environment: `Check if setting is correct`,
        }
      }
    }

    reporter.panic(`Accessing your Contentful space failed.
Try setting GATSBY_CONTENTFUL_OFFLINE=true to see if we can serve from cache.
${details ? `\n${details}\n` : ``}
Used options:
${formatPluginOptionsForCLI(pluginConfig.getOriginalPluginOptions(), errors)}`)
  }

  // Temporary replacement for `client.sync`. See details below, where this
  // function is called.
  async function getAllEntriesAndAssets() {
    const entriesPageSize = 50

    let entriesRemaining = true
    let skipEntries = 0
    let entries = []
    while (entriesRemaining) {
      console.log(
        `FETCHING ENTRIES ${skipEntries + 1} TO ${skipEntries +
          entriesPageSize}`
      )
      const fetchedEntries = await client.getEntries({
        include: 0,
        skip: skipEntries,
        limit: entriesPageSize,
        locale: `*`,
      })
      if (fetchedEntries.items.length) {
        entries = [...entries, ...fetchedEntries.items]
        skipEntries += entriesPageSize
      } else {
        entriesRemaining = false
      }
    }

    const assetsPageSize = 100

    let assetsRemaining = true
    let skipAssets = 0
    let assets = []
    while (assetsRemaining) {
      console.log(
        `FETCHING ASSETS ${skipAssets + 1} TO ${skipAssets + assetsPageSize}`
      )
      const fetchedAssets = await client.getAssets({
        skip: skipAssets,
        limit: assetsPageSize,
        locale: `*`,
      })
      if (fetchedAssets.items.length) {
        assets = [...assets, ...fetchedAssets.items]
        skipAssets += assetsPageSize
      } else {
        assetsRemaining = false
      }
    }

    // Manually resolve entries with each other and with assets
    const resolvedEntries = resolveResponse(
      {
        sys: { type: `Array` },
        includes: { Asset: assets },
        items: entries,
      },
      { itemEntryPoints: [`fields`] }
    )

    return {
      entries: resolvedEntries,
      assets,
      deletedEntries: [],
      deletedAssets: [],
    }
  }

  let currentSyncData
  try {
    // TODO: Re-enable the Sync API once Contentful fixes it. This change is due
    // to the fact that Contentful's Sync API doesn't respect its own response
    // size limit of 7MB. Since we can't tell the Sync API how many items to
    // fetch per "page", there's no way to avoid the Sync API breaking when the
    // articles in a single page are too big. Thus, we have to make our own
    // method of fetching all entries/assets, which we'll use until Contentful
    // has fixed this issue. For more info, see the email thread started on 14
    // Feb 2020, with the subject line "[Support] Re: Sync API returning
    // "Response size too big".
    // let query = syncToken ? { nextSyncToken: syncToken } : { initial: true }
    // currentSyncData = await client.sync(query)
    currentSyncData = await getAllEntriesAndAssets()
  } catch (e) {
    reporter.panic(`Fetching contentful data failed`, e)
  }

  // We need to fetch content types with the non-sync API as the sync API
  // doesn't support this.
  let contentTypes
  try {
    contentTypes = await pagedGet(client, `getContentTypes`)
  } catch (e) {
    console.log(`error fetching content types`, e)
  }
  console.log(`contentTypes fetched`, contentTypes.items.length)

  let contentTypeItems = contentTypes.items

  // Fix IDs on entries and assets, created/updated and deleted.
  contentTypeItems = contentTypeItems.map(c => normalize.fixIds(c))

  currentSyncData.entries = currentSyncData.entries.map(e => {
    if (e) {
      return normalize.fixIds(e)
    }
    return null
  })
  currentSyncData.assets = currentSyncData.assets.map(a => {
    if (a) {
      return normalize.fixIds(a)
    }
    return null
  })
  currentSyncData.deletedEntries = currentSyncData.deletedEntries.map(e => {
    if (e) {
      return normalize.fixIds(e)
    }
    return null
  })
  currentSyncData.deletedAssets = currentSyncData.deletedAssets.map(a => {
    if (a) {
      return normalize.fixIds(a)
    }
    return null
  })

  const result = {
    currentSyncData,
    contentTypeItems,
    defaultLocale,
    locales,
  }

  return result
}

/**
 * Gets all the existing entities based on pagination parameters.
 * The first call will have no aggregated response. Subsequent calls will
 * concatenate the new responses to the original one.
 */
function pagedGet(
  client,
  method,
  query = {},
  skip = 0,
  pageLimit = 1000,
  aggregatedResponse = null
) {
  return client[method]({
    ...query,
    skip: skip,
    limit: pageLimit,
    order: `sys.createdAt`,
  }).then(response => {
    if (!aggregatedResponse) {
      aggregatedResponse = response
    } else {
      aggregatedResponse.items = aggregatedResponse.items.concat(response.items)
    }
    if (skip + pageLimit <= response.total) {
      return pagedGet(
        client,
        method,
        query,
        skip + pageLimit,
        pageLimit,
        aggregatedResponse
      )
    }
    return aggregatedResponse
  })
}
