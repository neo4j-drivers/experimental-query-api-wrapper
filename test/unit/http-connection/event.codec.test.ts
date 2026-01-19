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
import { QueryApiEventTransformer, Event, HeaderEvent, RecordEvent, SummaryEvent, ErrorEvent } from '../../../src/http-connection/event.codec'
import { TransformStreamDefaultController } from 'stream/web'

describe('QueryApiEventTransformer', () => {

    it.each([
        ...headers(),
        ...records(),
        ...summaries(),
        ...errors()
    ])('should handle well formed events', (json, event) => {
        const transformer = new QueryApiEventTransformer()
        const { controller, spyOnEnqueue, spyOnError } = mockController()

        transformer.transform(json, controller)

        expect(spyOnError).not.toHaveBeenCalled()
        expect(spyOnEnqueue).toHaveBeenCalledTimes(1)
        expect(spyOnEnqueue).toHaveBeenCalledWith(event)
    })

    it('should emit error when not well formed json events', () => {
        const transformer = new QueryApiEventTransformer()
        const { controller, spyOnEnqueue, spyOnError } = mockController()

        transformer.transform('{ $event: "Header", _body: { fields: ["abc", "cbd"] } }', controller)

        expect(spyOnEnqueue).not.toHaveBeenCalled()
        expect(spyOnError).toHaveBeenCalled()
    })

    it.each(invalidEvents())('should emit error when not well formed event', (json) => {
        const transformer = new QueryApiEventTransformer()
        const { controller, spyOnEnqueue, spyOnError } = mockController()

        transformer.transform(json, controller)

        expect(spyOnEnqueue).not.toHaveBeenCalled()
        expect(spyOnError).toHaveBeenCalled()
    })

    it.each([
        ...headers(),
        ...records(),
        ...summaries(),
        ...errors()
    ])('should recover from error', (json, event) => {
        const transformer = new QueryApiEventTransformer()
        const { controller, spyOnEnqueue, spyOnError } = mockController()

        // Error happened
        transformer.transform('{ $event: "Header", _body: { fields: ["abc", "cbd"] } }', controller)
        expect(spyOnEnqueue).not.toHaveBeenCalled()
        expect(spyOnError).toHaveBeenCalled()

        // Other events came
        transformer.transform(json, controller)

        expect(spyOnEnqueue).toHaveBeenCalledTimes(1)
        expect(spyOnEnqueue).toHaveBeenCalledWith(event)
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

    function records(): [string, RecordEvent][] {
        return [
            ['{ "$event": "Record", "_body": [{ "$type": "Integer", "_value": "123" }, { "$type": "String", "_value": "abcde" }]}', { $event: 'Record', _body: [{ $type: 'Integer', _value: '123' }, { $type: 'String', _value: 'abcde' }]}],
            ['{ "$event": "Record", "_body": [{ "$type": "Integer", "_value": "123" }]}', { $event: 'Record', _body: [{ $type: 'Integer', _value: '123' }]}], 
            ['{ "$event": "Record", "_body": [] }', { $event: 'Record', _body: []}]
        ]
    }

    function summaries(): [string, SummaryEvent][] {
        return [
            ['{ "$event": "Summary", "_body": { "bookmarks": ["bm1", "bm2"] } }', { $event: 'Summary', _body: { bookmarks: ['bm1', 'bm2'] } }],
            ['{ "$event": "Summary", "_body": {} }', { $event: 'Summary', _body: {} }]
        ]
    }

    function errors(): [string, ErrorEvent][] {
        return [
            ['{ "$event": "Error", "_body": [{ "code": "Neo.Made.Up.Error", "message": "the error message"}, { "code": "Neo.Made.Up.Error2", "message": "the error message 2"}]}', { $event: 'Error', _body: [ { code: 'Neo.Made.Up.Error', message: 'the error message'}, { code: 'Neo.Made.Up.Error2', message: 'the error message 2'} ] }],
            ['{ "$event": "Error", "_body": [{ "code": "Neo.Made.Up.Error", "message": "the error message"}]}', { $event: 'Error', _body: [ { code: 'Neo.Made.Up.Error', message: 'the error message'} ] }], 
            ['{ "$event": "Error", "_body": [] }', { $event: 'Error', _body: [] }]
        ]
    }

    function invalidEvents(): [string][] {
        return [
            ['null'],
            ['"string"'],
            ['1'],
            ["{}"],
            ['{ "_body": {} }'],
            ['{ "$event": "Summary }'],
            ['{ "$event": "Summary, "_body": "string" }'],
            ['{ "$event": "Summary, "_body": 123 }'],
            ['{ "$event": 1, "_body": {} }'],
            ['{ "$event": "Summary", "body": {} }'],
            ['{ "event": "Summary", "_body": {} }']
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
