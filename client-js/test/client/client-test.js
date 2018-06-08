/*
 * Copyright 2018, TeamDev. All rights reserved.
 *
 * Redistribution and use in source and/or binary forms, with or without
 * modification, must retain the above copyright notice and the following
 * disclaimer.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

let assert = require("assert");

let ActorRequestFactory = require("../../src/client/actor-request-factory").ActorRequestFactory;
let BackendClient = require("../../src/client/backend-client").BackendClient;
let FirebaseClient = require("../../src/client/firebase-client").FirebaseClient;
let HttpClient = require("../../src/client/http-client").HttpClient;
let firebase = require("./test-firebase-app").devFirebaseApp;

let commands = require("../../proto/test/js/spine/web/test/commands_pb");
let task = require("../../proto/test/js/spine/web/test/task_pb");

let { TypeUrl, TypedMessage} = require("../../src/client/typed-message");
let httpClient = new HttpClient("https://spine-dev.appspot.com");
let requestFactory = new ActorRequestFactory("web-test-actor");
let backendClient = new BackendClient(httpClient,
                                      new FirebaseClient(firebase),
                                      requestFactory);

function creteTaskCommand(id, name, description) {
  let command = new commands.CreateTask();
  command.setId(id);
  command.setName(name);
  command.setDescription(description);

  let commandType = new TypeUrl("type.spine.io/spine.web.test.CreateTask");
  let typedCommand = new TypedMessage(command, commandType);

  return typedCommand;
}

function randomId(prefix) {
  let id = prefix + Math.round(Math.random() * 1000);
  let productId = new task.TaskId();
  productId.setValue(id);
  return productId;
}

describe("Client should", function () {
  this.timeout(120/*seconds*/ * 1000);

  it("send commands successfully", function (done) {
    let productId = randomId("spine-web-test-1-");
    let command = creteTaskCommand(productId, "Write tests", "client-js needs tests; write'em");
    backendClient.sendCommand(command, function() {
      let type = new TypeUrl("type.spine.io/spine.web.test.Task");
      let idType = new TypeUrl("type.spine.io/spine.web.test.TaskId");
      let typedId = new TypedMessage(productId, idType);
      backendClient.fetchById(type, typedId, data => {
        assert.equal(data.name, command.message.getName());
        assert.equal(data.description, command.message.getDescription());
        done();
      }, done);
    }, done, done);
  });

  it("fetch all the existing entities of given type", function (done) {
    let productId = randomId("spine-web-test-2-");
    let command = creteTaskCommand(productId, "Run tests", "client-js has tests; run'em");
    backendClient.sendCommand(command, function () {
      let type = new TypeUrl("type.spine.io/spine.web.test.Task");
      backendClient.fetchAll(type, function (data) {
        if (data.id.value === productId.getValue()) {
          done();
        }
      });
    }, done, done);
  });
});
