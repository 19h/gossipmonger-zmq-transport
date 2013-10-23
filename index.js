/*
         __                            ___             
        /\ \                     __  /'___\            
        \ \ \         __     __ /\_\/\ \__/  __  __    
         \ \ \  __  /'__`\ /'_ `\/\ \ \ ,__\/\ \/\ \   
          \ \ \L\ \/\  __//\ \L\ \ \ \ \ \_/\ \ \_\ \  
           \ \____/\ \____\ \____ \ \_\ \_\  \/`____ \ 
            \/___/  \/____/\/___L\ \/_/\/_/   `/___/> \
                             /\____/             /\___/
                             \_/__/              \/__/

        Copyright (c) 2013 by Legify UG. All Rights Reserved.

        [Portions/gossipmonger-tcp-transport] Copyright (c) 2013 Tristan Slominski

        Permission is hereby granted, free of charge, to any person
        obtaining a copy of this software and associated documentation
        files (the "Software"), to deal in the Software without
        restriction, including without limitation the rights to use,
        copy, modify, merge, publish, distribute, sublicense, and/or sell
        copies of the Software, and to permit persons to whom the
        Software is furnished to do so, subject to the following
        conditions:

        The above copyright notice and this permission notice shall be
        included in all copies or substantial portions of the Software.

        THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
        EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
        OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
        NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
        HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
        WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
        FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
        OTHER DEALINGS IN THE SOFTWARE.

*/
"use strict";

var events = require('events'),
        zmq = require('zmq'),
        util = require('util');

/*
 * `options`: _Object_
 * `host`: _String_ _(Default: `localhost`)_ Hostname or IP.
 * `port`: _Integer_ _(Default: 9742)_ Port number.
 */

var TcpTransport = module.exports = function TcpTransport(options) {
        var self = this;
        events.EventEmitter.call(self);

        options = options || {};

        self.host = options.host || 'localhost';
        self.port = options.port || 9742;

        self.server = null;
};

util.inherits(TcpTransport, events.EventEmitter);

/*
  * `options`: See `new TcpTransport(options)` `options`.
  * `callback`: See `tcpTransport.listen(callback)` `callback`.
  Return: _Object_ An instance of TcpTransport with server listening on host
          and port as specified in options or defaults.
*/

TcpTransport.listen = function listen(options, callback) {
        var tcpTransport = new TcpTransport(options);
        tcpTransport.listen(callback);
        return tcpTransport;
};

/*
  * `callback`: _Function_ _(Default: undefined)_ Optional callback to call once
      the server is stopped.
*/

TcpTransport.prototype.close = function close(callback) {
        var self = this;
        if (self.server)
                self.server.close(callback);
};

/*
 * `remotePeer`: _Object_ Peer to send rpc to.
 * `transport`: _Object_ TCP transport data.
 * `host`: _String_ Host to connect to.
 * `port`: _Integer_ Port to connect to.
 * `localPeer`: _Object_ Sender peer.
 * `id`: _String_ Sender peer id.
 * `transport`: _Object_ TCP transport data.
 * `host`: _String_ Host to connect to.
 * `port`: _Integer_ Port to connect to.
 * `deltasToSend`: _Any_ Deltas to send.
 */

TcpTransport.prototype.deltas = function deltas(remotePeer, localPeer, deltasToSend) {
        var self = this;

        self.rpc(remotePeer, {
                deltas: deltasToSend,
                sender: {
                        id: localPeer.id,
                        transport: localPeer.transport
                }
        });
};

/*
 * `remotePeer`: _Object_ Peer to send rpc to.
 * `transport`: _Object_ TCP transport data.
 * `host`: _String_ Host to connect to.
 * `port`: _Integer_ Port to connect to.
 * `localPeer`: _Object_ Sender peer.
 * `id`: _String_ Sender peer id.
 * `transport`: _Object_ TCP transport data.
 * `host`: _String_ Host to connect to.
 * `port`: _Integer_ Port to connect to.
 * `digestToSend`: _Any_ Digest to send.
 */

TcpTransport.prototype.digest = function digest(remotePeer, localPeer, digestToSend) {
        var self = this;

        self.rpc(remotePeer, {
                digest: digestToSend,
                sender: {
                        id: localPeer.id,
                        transport: localPeer.transport
                }
        });
};

/*
  * `options`: _Object_
    * `host`: _String_ _(Default: as specified on construction)_ Hostname or IP
            to listen on.
    * `port`: _Integer_ _(Default: as specified on construction)_ Port number
            to listen on.
  * `callback`: _Function_ _(Default: undefined)_ Optional callback to call once
      the server is up.
*/

TcpTransport.prototype.listen = function listen(options, callback) {
        var self = this;

        // options are optional
        if (typeof options === 'function') {
                callback = options;
                options = {};
        }

        self.server = zmq.socket('pull');

        var listenHost = options.host || self.host;
        var listenPort = options.port || self.port;

        self.server.connect('tcp://' + listenHost + ':' + listenPort);

        self.server.on('message', function(msg){
                try {
                        data = JSON.parse(msg.toString("utf8"));
                } catch (exception) {
                        // ignore
                }
                if (data.deltas) {
                        self.emit('deltas', data.sender, data.deltas);
                } else if (data.digest) {
                        self.emit('digest', data.sender, data.digest);
                }
        });
};

/*
  * `remotePeer`: _Object_ Peer to send rpc to.
    * `transport`: _Object_ TCP transport data.
      * `host`: _String_ Host to connect to.
      * `port`: _Integer_ Port to connect to.
  * `payload`: _String_ or _Object_ Payload is send on the wire. If an _Object_
          is provided, it will be `JSON.stringify()`'ed.
*/

TcpTransport.prototype.rpc = function rpc(remotePeer, payload) {
        var self = this;
        if (!remotePeer || !remotePeer.transport || !remotePeer.transport.host || !remotePeer.transport.port) {

                return self.emit('error', new Error("malformed remotePeer"));
        }

        var client = zmq.socket('push');

        client.bindSync('tcp://' + remotePeer.transport.host + ':' + remotePeer.transport.port);
        
        if (typeof payload != "string")
                payload = JSON.stringify(payload);

        return sock.send(payload + '\r\n');
};