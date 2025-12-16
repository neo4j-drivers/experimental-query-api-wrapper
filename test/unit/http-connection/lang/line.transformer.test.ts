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
import LineTransformer from "../../../../src/http-connection/lang/line.transformer";
import { TransformStreamDefaultController } from 'stream/web'

describe('LineTransformer', () => {


    it.each(lines())('should handle each line in a chunk', (lines) => {
        const transformer = new LineTransformer()
        const { controller, spyOnError, spyOnEnqueue } = mockController()

        for (const line of lines) {
            transformer.transform(line + '\n', controller)
        }

        expect(spyOnEnqueue).toHaveBeenCalledTimes(lines.length)

        lines.forEach((line, index) => expect(spyOnEnqueue).toHaveBeenNthCalledWith(index + 1, line))

        expect(spyOnError).not.toHaveBeenCalled()
    })

    it.each(lines())('should handle each line split between chunks', (lines) => {
        const transformer = new LineTransformer()
        const { controller, spyOnError, spyOnEnqueue } = mockController()

        for (const line of lines) {
            const parts = splitRandomParts(line, 4)

            for (const part of parts) {
                transformer.transform(part, controller)
            }
            transformer.transform('\n', controller)
        }

        expect(spyOnEnqueue).toHaveBeenCalledTimes(lines.length)

        lines.forEach((line, index) => expect(spyOnEnqueue).toHaveBeenNthCalledWith(index + 1, line))

        expect(spyOnError).not.toHaveBeenCalled()
    })

    it.each(lines())('should handle each line split between chunks sharing some chunk', (lines) => {
        const transformer = new LineTransformer()
        const { controller, spyOnError, spyOnEnqueue } = mockController()

        let rest: string | undefined = undefined
        for (const line of lines) {
            const [first, ...parts] = splitRandomParts(line, 4)
            const last = parts.pop() ?? ""
            
            if (rest != null) {
                transformer.transform(rest + '\n' + first, controller)
            } else {
                transformer.transform(first, controller)
            }
            
            for (const part of parts) {    
                transformer.transform(part, controller)
            }
            
            rest = last
        }

        if (rest != null) {
            transformer.transform(rest + '\n', controller)
        }
        
        expect(spyOnEnqueue).toHaveBeenCalledTimes(lines.length)

        lines.forEach((line, index) => expect(spyOnEnqueue).toHaveBeenNthCalledWith(index + 1, line))

        expect(spyOnError).not.toHaveBeenCalled()
    })

    function lines () {
        return [
            [["i'm a single line"]],
            [["the first", "the second 29291010"]],
            [["the first", "the second 29291010", "´`'132097818981!/(/#(&&€%&!()/!()&/!\t"]]
        ]
    }

    function mockController() {
        const controller: TransformStreamDefaultController<string> = {
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

    function splitRandomParts(str:string, n:number) {
        if (n <= 0 || n > str.length) {
          throw new Error("n must be between 1 and the length of the string");
        }
      
        // Choose n−1 random breakpoints
        const points = [...Array(n - 1)]
          .map(() => Math.floor(Math.random() * (str.length - 1)) + 1);
      
        points.sort((a, b) => a - b);
      
        const parts = [];
        let last = 0;
      
        for (const p of points) {
          parts.push(str.slice(last, p));
          last = p;
        }
        parts.push(str.slice(last));
      
        return parts;
      }
})
