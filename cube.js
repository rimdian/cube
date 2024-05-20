const MysqlDriver = require('@cubejs-backend/mysql-driver');
const axios = require('axios');
const https = require('https');
const _ = require('lodash');
if (process.env.NODE_ENV !== 'production') {
    require('dotenv').config(); // required for development with .env file
}

const refreshSchemaEveryXsecs = process.env.NO_CACHE ? 1 : 60;
const lastSchemaUpdate = {}

const espaceQuotes = (str) => {
    return str.replace(/'/g, "\\'")
}

const agent = new https.Agent({
    keepAlive: true,
    timeout: 60 * 1000, //10secs
    rejectUnauthorized: false
})

class RMDFileRepository {

    constructor(securityContext) {
        // securityContext should contain the workspace_id
        this.securityContext = securityContext;
    }

    async dataSchemaFiles() {
        // console.log('Fetching schema files from the API ' + this.securityContext.schema_url)

        return new Promise((resolve, reject) => {
            // console.log('Fetching schema files from the API ' + this.securityContext.schema_url)

            axios.get(this.securityContext.schema_url, {
                httpsAgent: agent,
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                }
            }).then((response) => {
                // console.log('res', response.data)

                if (response.status !== 200) {
                    return reject(new Error(`Failed to fetch schema files from the API: ${response.status}`));
                }
                if (!response.data) {
                    return reject(new Error('Failed to fetch schema files from the API: no data'));
                }

                // convert JSON files to JS "function" files
                const result = []

                response.data.forEach((file) => {
                    // console.log('file', file)
                    // console.log('file.content', file.content)
                    // console.log('file.content', JSON.stringify(file.content))
                    const contentSchema = JSON.parse(file.content)
                    const measures = []
                    const dimensions = []
                    const segments = []
                    const joins = []

                    // build measures
                    _.forEach(contentSchema.measures, (measure, key) => {
                        const properties = [
                            `title: '${espaceQuotes(measure.title)}'`,
                            `type: '${measure.type}'`,
                            `description: '${espaceQuotes(measure.description)}'`
                        ]

                        if (measure.sql && measure.sql !== '') {
                            properties.push(`sql: \`${espaceQuotes(measure.sql)}\``)
                        }
                        if (measure.drillMembers && measure.drillMembers.length > 0) {
                            properties.push(`drillMembers: [${measure.drillMembers.map((drillMember) => `'${espaceQuotes(drillMember)}'`).join(', ')}]`)
                        }
                        if (measure.filters && measure.filters.length > 0) {

                            properties.push(`filters: [${measure.filters.map((filter) => `{sql: \`${espaceQuotes(filter.sql)}\`}`).join(', ')}]`)
                        }
                        if (measure.format && measure.format !== '') {
                            properties.push(`format: '${espaceQuotes(measure.format)}'`)
                        }
                        if (measure.rollingWindow) {
                            const rollingWindowProperties = []
                            if (measure.rollingWindow.trailing && measure.rollingWindow.trailing !== '') {
                                rollingWindowProperties.push(`trailing: \`${espaceQuotes(measure.rollingWindow.trailing)}\``)
                            }
                            if (measure.rollingWindow.leading && measure.rollingWindow.leading !== '') {
                                rollingWindowProperties.push(`leading: \`${espaceQuotes(measure.rollingWindow.leading)}\``)
                            }
                            if (measure.rollingWindow.offset && measure.rollingWindow.offset !== '') {
                                rollingWindowProperties.push(`offset: \`${espaceQuotes(measure.rollingWindow.offset)}\``)
                            }
                            properties.push(`rollingWindow: {${rollingWindowProperties.join(', ')}}`)
                        }
                        if (measure.shown && !measure.shown) {
                            properties.push(`shown: false`)
                        }
                        if (measure.meta) {
                            // JSON encode the meta object
                            properties.push(`meta: '${espaceQuotes(JSON.stringify(measure.meta))}'`)
                        }

                        measures.push(`${key}: {${properties.join(', ')}}`)
                    })

                    // build dimensions
                    _.forEach(contentSchema.dimensions, (dimension, key) => {

                        const properties = [
                            `title: '${espaceQuotes(dimension.title)}'`,
                            `type: '${dimension.type}'`,
                            `description: '${espaceQuotes(dimension.description)}'`
                        ]

                        if (dimension.sql && dimension.sql !== '') {
                            properties.push(`sql: \`${espaceQuotes(dimension.sql)}\``)
                        }
                        if (dimension.primaryKey) {
                            properties.push(`primaryKey: true`)
                        }
                        if (dimension.format && dimension.format !== '') {
                            properties.push(`format: '${espaceQuotes(dimension.format)}'`)
                        }
                        if (dimension.shown && !dimension.shown) {
                            properties.push(`shown: false`)
                        }
                        if (dimension.meta) {
                            // JSON encode the meta object
                            properties.push(`meta: '${espaceQuotes(JSON.stringify(dimension.meta))}'`)
                        }
                        if (dimension.subquery) {
                            properties.push(`subquery: true`)
                        }
                        if (dimension.propagateFiltersToSubQuery) {
                            properties.push(`propagateFiltersToSubQuery: true`)
                        }
                        if (dimension.case) {
                            const caseProperties = []
                            if (dimension.case.when && dimension.case.when.length > 0) {
                                caseProperties.push(`when: [${dimension.case.when.map((when) => `{sql: \`${espaceQuotes(when.sql)}\`, label: '${espaceQuotes(when.label)}'}`).join(', ')}]`)
                            }
                            if (dimension.case.else) {
                                caseProperties.push(`else: {label: '${espaceQuotes(dimension.case.else.label)}'}`)
                            }
                            properties.push(`case: {${caseProperties.join(', ')}}`)
                        }

                        dimensions.push(`${key}: {${properties.join(', ')}}`)
                    })

                    // build segments
                    _.forEach(contentSchema.segments, (segment, key) => {
                        segments.push(`${key}: {sql: \`${espaceQuotes(segment.sql)}\`}`)
                    })

                    // build joins
                    _.forEach(contentSchema.joins, (join, key) => {
                        joins.push(`${key}: {sql: \`${espaceQuotes(join.sql)}\`, relationship: '${espaceQuotes(join.relationship)}'}`)
                    })

                    const content = `cube(\`${file.fileName}\`, {sql: '${contentSchema.sql}', title: '${espaceQuotes(contentSchema.title)}', description: '${espaceQuotes(contentSchema.description)}', segments: {` + segments.join(`,`) + `}, joins: {` + joins.join(`,`) + `}, measures: {` + measures.join(`,`) + `}, dimensions: {` + dimensions.join(`,`) + `}});`

                    result.push({
                        // add .js extension otherwise it doesnt work
                        fileName: file.fileName + '.js',
                        content: content
                    })
                })
                // console.log('result', result)
                resolve(result)
            }).catch(reject)
        })
    };
    // return [
    //     { fileName: 'Users', content: Users },
    //     { fileName: 'Sessions', content: Sessions },
    //     { fileName: 'Devices', content: Devices },
    //     { fileName: 'Orders', content: Orders },
    //     { fileName: 'Carts', content: Carts },
    //     ...
    // ];
}

// https://cube.dev/docs/reference/configuration/config
module.exports = {
    telemetry: false,
    cacheAndQueueDriver: 'memory', // disable cubestore, singlestore is fast enough
    scheduledRefreshTimer: false,

    // called once per tenant
    // Used to tell Cube which database type is used to store data for a tenant.
    dbType: 'mysql',

    // call on each request
    // Used to tell Cube which tenant is making the current request.
    contextToAppId: ({ securityContext }) => {
        if (!securityContext) return 'CUBEJS_APP_ANONYMOUS'
        else return `CUBEJS_APP_${securityContext.workspace_id}`
    },

    contextToOrchestratorId: ({ securityContext }) => `CUBEJS_APP_${securityContext.workspace_id}`,

    // called once per tenant
    // Used to tell Cube which database schema to use to store pre-aggregations for a tenant
    preAggregationsSchema: ({ securityContext }) => {
        if (!securityContext) return 'CUBEJS_APP_ANONYMOUS'
        else return `pre_aggregations_${securityContext.workspace_id}`
    },

    // called once per datasource
    // Used to tell Cube which database driver is used for a data source
    driverFactory: ({ securityContext }) => {
        const cfg = {
            readOnly: true,
            database: process.env.DB_PREFIX + securityContext.workspace_id,
        }
        console.log('running cube.js driverFactory')

        return new MysqlDriver(cfg)
    },

    schemaVersion: ({ securityContext }) => {
        const currentTimestamp = new Date().getTime()

        if (!securityContext || !securityContext.workspace_id) return 0

        if (!lastSchemaUpdate[securityContext.workspace_id]) {
            lastSchemaUpdate[securityContext.workspace_id] = currentTimestamp
            return lastSchemaUpdate[securityContext.workspace_id]
        }

        // check if the lastSchemaUpdate is older than the cache timeout
        if (lastSchemaUpdate[securityContext.workspace_id] < new Date(currentTimestamp - (1000 * refreshSchemaEveryXsecs)).getTime()) {
            // update the lastSchemaUpdate
            lastSchemaUpdate[securityContext.workspace_id] = currentTimestamp
        }

        return lastSchemaUpdate[securityContext.workspace_id]
    },

    // called once per tenant
    // Used to tell Cube which data schema files to use for a tenant.
    repositoryFactory: ({ securityContext }) => new RMDFileRepository(securityContext),

    scheduledRefreshContexts: async () => [
        // {
        //     securityContext: {},
        // },
    ],
}