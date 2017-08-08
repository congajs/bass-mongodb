const AdapterIntegrationSpec = require('../node_modules/bass/spec/AdapterIntegrationSpec');

describe('bass-mongodb', AdapterIntegrationSpec('bass-mongodb', {
    connections: {
        default: {
            adapter: 'bass-mongodb',
            host: 'localhost',
            database: 'test',
            port: '27017'
        }
    }
}));
