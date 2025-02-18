
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
import config from './config'
import { when, withSession } from './test.utils'
import neo4j, { Wrapper } from '../../src'

when(config.version >= 5.26, () => describe('transactions', () => {
  let wrapper: Wrapper

  beforeAll(async () => {
    await config.startNeo4j()
  }, 120_000) // long timeout since it may need to download docker image

  afterAll(async () => {
    await config.stopNeo4j()
  }, 20000)

  beforeEach(() => {
    wrapper = neo4j.wrapper(
      `${config.httpScheme}://${config.hostname}:${config.httpPort}`,
      neo4j.auth.basic(config.username, config.password),
      { 
        logging: config.loggingConfig
      }
    )
  })

  afterEach(async () => {
    for await (const session of withSession(wrapper, { database: config.database })) {
      await session.run('MATCH (n) DETACH DELETE n')
    }
    await wrapper?.close()
  })

  describe('unmanaged', () => {
    it.each([
      [0],
      [1],
      [2],
      [5]
    ])('should be able to run %s queries and commit a read tx', async (queries) => {
      for await (const session of withSession(wrapper, { database: config.database, defaultAccessMode: 'READ' })) {
        const tx = await session.beginTransaction()
        try  {
          for (let i = 0; i < queries; i++) {
            await expect(tx.run('RETURN $i AS a', { i })).resolves.toBeDefined()
          }
          await expect(tx.commit()).resolves.toBe(undefined)
        } finally {
          await tx.close()
        }
      }
    })

    it.each([
      [0],
      [1],
      [2],
      [5]
    ])('should be able to run %s queries and commit a write tx', async (queries) => {
      for await (const session of withSession(wrapper, { database: config.database, defaultAccessMode: 'WRITE' })) {
        const tx = await session.beginTransaction()
        try  {
          for (let i = 0; i < queries; i++) {
            await expect(tx.run('CREATE (n:Person{a:$i}) RETURN n.a AS a', { i })).resolves.toBeDefined()
          }

          await expect(tx.commit()).resolves.toBe(undefined)
        } finally {
          await tx.close()
        }
      }
    })

    it('should be able to rollback a tx', async () => {
      for await (const session of withSession(wrapper, { database: config.database, defaultAccessMode: 'WRITE' })) {
        const tx = await session.beginTransaction()
        try  {
          const a = 'A'
          // CREATING NODE
          await expect(tx.run('CREATE (n:Person{a:$a}) RETURN n.a AS a', { a })).resolves.toBeDefined()
          
          // CHECK IF THE NODE IS ON THE DATABASE
          const { records: [record] } = await tx.run('MATCH (n:Person{a:$a}) RETURN n.a AS a', { a })
          expect(record.get('a')).toBe(a)

          // ROLLBACK
          await expect(tx.rollback()).resolves.toBe(undefined)

          // CHECK IF USER IS NOT ON THE DB
          const { records } = await session.run('MATCH (n:Person{a:$a}) RETURN n.a AS a', { a })
          expect(records.length).toBe(0)
        } finally {
          await tx.close()
        }
      }
    })
  })

  describe('managed', () => {
    it.each([
      [0],
      [1],
      [2],
      [5]
    ])('should be able to run %s queries using executeRead', async (queries) => {
      for await (const session of withSession(wrapper, { database: config.database })) {
        await expect(session.executeRead(async tx => {
          for (let i = 0; i < queries; i++) {
            await expect(tx.run('RETURN $i AS a', { i })).resolves.toBeDefined()
          }
        })).resolves.toBe(undefined)
      }
    })

    it.each([
      [0],
      [1],
      [2],
      [5]
    ])('should be able to run %s queries using executeWrite', async (queries) => {
      for await (const session of withSession(wrapper, { database: config.database, })) {
        await expect(session.executeWrite(async tx => {
          for (let i = 0; i < queries; i++) {
            await expect(tx.run('CREATE (n:Person{a:$i}) RETURN n.a AS a', { i })).resolves.toBeDefined()
          }
        })).resolves.toBe(undefined)
      }
    })

    it('should be able to handle password rotation on executeWrite', async () => {
      let password = config.password + 'wrong'
      let passwordCall = 0
      wrapper = neo4j.wrapper(`${config.httpScheme}://${config.hostname}:${config.httpPort}`,
        neo4j.authTokenManagers.basic({ tokenProvider: async () => {
            try {
              return neo4j.auth.basic(config.username, password)
            } finally {
              passwordCall++
              password = config.password
            }
          }
        }),
        {
          logging: config.loggingConfig
        }
      )
  
      for await (const session of withSession(wrapper, { database: config.database, })) {
        await expect(session.executeWrite((tx) => tx.run('CREATE (:Person {name: $name })', { name: 'Gregory Irons'}))).resolves.toBeDefined()

        expect(passwordCall).toBe(2)
      }
    })

    it('should be able to run a read query using executeQuery', async () => {
      const i = 34
      await expect(wrapper.executeQuery('RETURN $i AS a', { i }, {
        database: config.database,
        routing: 'READ'
      })).resolves.toBeDefined()
    })

    it('should be able to run a write query using executeQuery', async () => {
      const i = 34
      await expect(wrapper.executeQuery('CREATE (n:Person{a:$i}) RETURN n.a AS a', { i }, {
        database: config.database,
        routing: 'WRITE'
      })).resolves.toBeDefined()
    })
  })
}))