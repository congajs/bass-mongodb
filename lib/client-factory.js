// local modules
var ClientClass = require('./client');

module.exports = {

	/**
	 * Get a bass Client for a bass ManagerDefinition
	 * @param {ManagerDefinition} definition
	 * @returns {Client} Instance
	 */
	factory: function(definition)
	{
		var _class = this.getClass(definition);
		return new _class(definition.connection, definition.logger);
	} ,

	/**
	 * Get a bass Client Class for a bass ManagerDefinition
	 * @param {ManagerDefinition} definition
	 * @returns {Client} Constructor
	 */
	getClass: function(definition)
	{
		var _class;
		switch (definition.driver)
		{
			default :
			{
				_class = ClientClass;
				break;
			}
		}
		return _class;
	}
};