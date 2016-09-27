/*
 * This file is part of the bass-mongodb library.
 *
 * (c) Anthony Matarazzo <email@anthonymatarazzo.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

// bass modules
const BassIdStrategy = require('../../bass').IdStrategy;

module.exports = class IdStrategy extends BassIdStrategy {

	constructor(format) {
		super(format);

		this.commands = {
			pid: function() {
				return BassIdStrategy.prototype.commands.pid.apply(this, arguments).toString(16);
			} ,
			time: function() {
				return BassIdStrategy.prototype.commands.time.apply(this, arguments).toString(16);
			},
			random: function() {
				return (Math.floor(Math.random() * (9999999 - 1000000)) + 1000000).toString(16);
			}
		};
	}

	generate() {
		var id = BassIdStrategy.prototype.generate.apply(this, arguments);
		if (id) {
			var len = id.length;
			if (len !== 0) {
				while (len < 24) {
					id += Math.floor(Math.random() * 10);
					len++;
				}
				if (len > 24) {
					id = id.substr(0, 24);
				}
			}
		}
		return id;
	}

}
