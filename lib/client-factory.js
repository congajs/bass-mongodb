// local modules
const ClientClass = require('./client');


module.exports = class ClientFactory {

	/**
	 * Get a bass Client for a bass ManagerDefinition
	 * @param {ManagerDefinition} definition
	 * @param {Connection|undefined} connection The connection to use (if not provided, the connection comes from the definition)
	 * @returns {Client} Instance
	 */
	factory(definition, connection) {
		var _class = this.getClass(definition);
		return new _class(connection || definition.connection, definition.logger);
	}

	/**
	 * Get a bass Client Class for a bass ManagerDefinition
	 * @param {ManagerDefinition} definition
	 * @returns {Client} Constructor
	 */
	getClass(definition) {
		var _class;
		switch (definition.driver) {
			default: {
				_class = ClientClass;
				break;
			}
		}
		return _class;
	}
}