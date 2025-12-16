/**
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { QueryApiEventTransformer, Event, HeaderEvent } from '../../../src/http-connection/event.codec'
import { TransformStreamDefaultController } from 'stream/web'

describe('QueryApiEventTransformer', () => {

    it.each(headers())('should handle Header', (json, header) => {
        const transformer = new QueryApiEventTransformer()
        const { controller, spyOnEnqueue, spyOnError } = mockController()

        transformer.transform(json, controller)

        expect(spyOnError).not.toHaveBeenCalled()
        expect(spyOnEnqueue).toHaveBeenCalledTimes(1)
        expect(spyOnEnqueue).toHaveBeenCalledWith(header)
    })

    function headers(): [string, HeaderEvent][] {
        return [
            ['{ "$event": "Header", "_body": { "fields": ["abc", "cbd"] } }', { $event: "Header", _body: { fields: ["abc", "cbd"] } }],
            ['{ "$event": "Header", "_body": { "fields": ["abc"] } }', { $event: "Header", _body: { fields: ["abc"] } }],
            ['{ "$event": "Header", "_body": { "fields": [] } }', { $event: "Header", _body: { fields: [] } }],
            // This shouldn't happen in the current use of the events. 
            // However, this is supported for case when events are using on start and finish transactions.
            ['{ "$event": "Header", "_body": { } }', { $event: "Header", _body: {  } }]
        ]
    }

    function mockController() {
        const controller: TransformStreamDefaultController<Event> = {
            desiredSize: null,
            enqueue(_chunk) {
                
            },
            error(_reason) {
                
            }, 
            terminate() {
                
            },
        }
        const spyOnEnqueue = jest.spyOn(controller, 'enqueue' )
        const spyOnError = jest.spyOn(controller, 'error')

        return {controller, spyOnEnqueue, spyOnError}
    }
})