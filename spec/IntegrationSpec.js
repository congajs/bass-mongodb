const AdapterIntegration = require('../node_modules/bass/spec/AdapterIntegration');

describe('bass-mongodb', AdapterIntegration('bass-mongodb', {
    connections: {
        default: {
            adapter: 'bass-mongodb',
            host: 'localhost',
            database: 'test',
            port: '27017'
        }
    }
}));
