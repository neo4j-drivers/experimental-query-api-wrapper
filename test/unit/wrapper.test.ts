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

import { auth } from 'neo4j-driver-core'
import { wrapper } from '../../src'

let capturedConfigs: any[] = []

jest.mock('../../src/http-connection', () => {
    const actual = jest.requireActual('../../src/http-connection')
    const OriginalProvider = actual.HttpConnectionProvider

    class MockHttpConnectionProvider extends OriginalProvider {
        constructor(config: any, ...rest: any[]) {
            super(config, ...rest)
            capturedConfigs.push(config)
        }
    }

    return {
        ...actual,
        HttpConnectionProvider: MockHttpConnectionProvider
    }
})

describe('wrapper()', () => {
    beforeEach(() => {
        capturedConfigs = []
    })

    describe('URL path handling', () => {
        it.each([
            ['http://localhost:7474', ''],
            ['http://localhost:7474/', ''],
            ['http://localhost:7474/my-proxy', '/my-proxy'],
            ['http://localhost:7474/deep/nested/path', '/deep/nested/path'],
            ['http://localhost:7474/trailing/', '/trailing'],
            ['https://localhost:7473/path', '/path'],
            ['http://localhost:7474/a/b/c/d', '/a/b/c/d'],
        ])('should pass path to HttpConnectionProvider for URL %s', async (url, expectedPath) => {
            const w = wrapper(url, auth.basic('neo4j', 'password'))

            try {
                // The provider is created lazily by the Driver, force it
                await w.supportsMultiDb()

                expect(capturedConfigs.length).toBe(1)
                expect(capturedConfigs[0].path).toBe(expectedPath)
            } finally {
                await w.close()
            }
        })

        it('should construct correct query endpoint with path', async () => {
            const w = wrapper('http://localhost:7474/my-proxy', auth.basic('neo4j', 'password'))

            try {
                await w.supportsMultiDb()

                expect(capturedConfigs.length).toBe(1)
                const provider = capturedConfigs[0]
                expect(provider.path).toBe('/my-proxy')
                expect(provider.scheme).toBe('http')
            } finally {
                await w.close()
            }
        })

        it('should construct correct query endpoint without path', async () => {
            const w = wrapper('http://localhost:7474', auth.basic('neo4j', 'password'))

            try {
                await w.supportsMultiDb()

                expect(capturedConfigs.length).toBe(1)
                expect(capturedConfigs[0].path).toBe('')
            } finally {
                await w.close()
            }
        })

        it('should construct correct query endpoint for https with path', async () => {
            const w = wrapper('https://localhost:7473/proxy/path', auth.basic('neo4j', 'password'))

            try {
                await w.supportsMultiDb()

                expect(capturedConfigs.length).toBe(1)
                expect(capturedConfigs[0].path).toBe('/proxy/path')
                expect(capturedConfigs[0].scheme).toBe('https')
            } finally {
                await w.close()
            }
        })
    })

    describe('scheme validation', () => {
        it('should accept http URLs', async () => {
            const w = wrapper('http://localhost:7474', auth.basic('neo4j', 'password'))
            await w.close()
        })

        it('should accept https URLs', async () => {
            const w = wrapper('https://localhost:7473', auth.basic('neo4j', 'password'))
            await w.close()
        })

        it('should reject unknown schemes', () => {
            expect(() => wrapper('bolt://localhost:7687', auth.basic('neo4j', 'password'))).toThrow('Unknown scheme')
        })
    })
})
