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

import { newError, Node, Relationship, int, error, types, Integer, Time, Date, LocalTime, Point, DateTime, LocalDateTime, Duration, isInt, isPoint, isDuration, isLocalTime, isTime, isDate, isLocalDateTime, isDateTime, isRelationship, isPath, isNode, isPathSegment, Path, PathSegment, internal, isUnboundRelationship, isVector } from "neo4j-driver-core"
import { RunQueryConfig } from "neo4j-driver-core/types/connection"
import { NEO4J_QUERY_CONTENT_TYPE, encodeAuthToken, encodeTransactionBody } from "./codec"
import TypedJsonCodec, { Counters, NotificationShape, ProfiledQueryPlan, RawQueryValue } from "./types.codec"


export type RawQueryData = {
    fields: string[]
    values: RawQueryValue[][]
}

export type RawQuerySuccessResponse = {
    data: RawQueryData
    counters: Counters
    bookmarks: string[]
    profiledQueryPlan?: ProfiledQueryPlan
    queryPlan?: ProfiledQueryPlan
    notifications?: NotificationShape[]
    [str: string]: unknown
}

export type RawQueryError = {
    code: string,
    message: string
    error?: string
}


export type RawQueryFailuresResponse = {
    errors: RawQueryError[]
}

export type RawQueryResponse = RawQuerySuccessResponse | RawQueryFailuresResponse

export class QueryResponseCodec {

    static of(
        config: types.InternalConfig,
        contentType: string,
        response: RawQueryResponse): QueryResponseCodec {

        if (isSuccess(response)) {
            if (contentType === NEO4J_QUERY_CONTENT_TYPE) {
                return new QuerySuccessResponseCodec(TypedJsonCodec.of(contentType, config), response)
            }
            return new QueryFailureResponseCodec(newError(
                `Wrong content-type. Expected "${NEO4J_QUERY_CONTENT_TYPE}", but got "${contentType}".`,
                error.PROTOCOL_ERROR
            ))
        }

        return new QueryFailureResponseCodec(response.errors?.length > 0 ?
            newError(
                response.errors[0].message,
                // TODO: REMOVE THE ?? AND .ERROR WHEN SERVER IS FIXED
                response.errors[0].code ?? response.errors[0].error
            ) :
            newError('Server replied an empty error response', error.PROTOCOL_ERROR))
    }

    get error(): Error | undefined {
        throw new Error('Not implemented')
    }

    get keys(): string[] {
        throw new Error('Not implemented')
    }

    get meta(): Record<string, unknown> {
        throw new Error('Not implemented')
    }

    *stream(): Generator<any[]> {
        throw new Error('Not implemented')
    }
}

class QuerySuccessResponseCodec extends QueryResponseCodec {

    constructor(
        private readonly _typedJsonCodec: TypedJsonCodec,
        private readonly _response: RawQuerySuccessResponse) {
        super()
    }

    get error(): Error | undefined {
        return undefined
    }

    get keys(): string[] {
        return this._response.data.fields
    }

    *stream(): Generator<any[]> {
        while (this._response.data.values.length > 0) {
            const value =  this._response.data.values.shift()
            if (value != null) {
                yield value.map(this._typedJsonCodec.decodeValue.bind(this._typedJsonCodec))
            }
        } 
        return
    }

    get meta(): Record<string, unknown> {
        return {
            bookmark: this._response.bookmarks,
            stats: this._typedJsonCodec.decodeStats(this._response.counters),
            profile: this._response.profiledQueryPlan != null ?
                this._typedJsonCodec.decodeProfile(this._response.profiledQueryPlan) : null,
            plan: this._response.queryPlan != null ?
                this._typedJsonCodec.decodeProfile(this._response.queryPlan) : null,
            notifications: this._response.notifications
        }
    }

}

class QueryFailureResponseCodec extends QueryResponseCodec {
    constructor(private readonly _error: Error) {
        super()
    }

    get error(): Error | undefined {
        return this._error
    }

    get keys(): string[] {
        throw this._error
    }

    get meta(): Record<string, unknown> {
        throw this._error
    }

    stream(): Generator<any[], any, unknown> {
        throw this._error
    }
}

export type QueryRequestCodecConfig = Pick<RunQueryConfig, 'bookmarks' | 'txConfig' | 'mode' | 'impersonatedUser'>

export class QueryRequestCodec {
    private _body?: Record<string, unknown>

    static of(
        auth: types.AuthToken,
        query: string,
        parameters?: Record<string, unknown> | undefined,
        config?: QueryRequestCodecConfig | undefined
    ): QueryRequestCodec {
        return new QueryRequestCodec(auth, query, parameters, config)
    }

    private constructor(
        private _auth: types.AuthToken,
        private _query: string,
        private _parameters?: Record<string, unknown> | undefined,
        private _config?: QueryRequestCodecConfig | undefined
    ) {

    }

    get contentType(): string {
        return NEO4J_QUERY_CONTENT_TYPE
    }

    get accept(): string {
        return `${NEO4J_QUERY_CONTENT_TYPE}, application/json`
    }

    get authorization(): string {
        return encodeAuthToken(this._auth)
    }

    get body(): Record<string, unknown> {
        if (this._body != null) {
            return this._body
        }

        this._body = {
            statement: this._query,
            includeCounters: true,
            ...encodeTransactionBody(this._config)
        }

        if (this._parameters != null && Object.getOwnPropertyNames(this._parameters).length !== 0) {
            this._body.parameters = this._encodeParameters(this._parameters!)
        }

        return this._body
    }

    _encodeParameters(parameters: Record<string, unknown>): Record<string, RawQueryValue> {
        const encodedParams: Record<string, RawQueryValue> = {}
        for (const k of Object.keys(parameters)) {
            if (Object.prototype.hasOwnProperty.call(parameters, k)) {
                encodedParams[k] = this._encodeValue(parameters[k])
            }
        }
        return encodedParams
    }

    _encodeValue(value: unknown): RawQueryValue {
        if (value === null) {
            return { $type: 'Null', _value: null }
        } else if (value === true || value === false) {
            return { $type: 'Boolean', _value: value }
        } else if (typeof value === 'number') {
            return { $type: 'Float', _value: value.toString() }
        } else if (typeof value === 'string') {
            return { $type: 'String', _value: value }
        } else if (typeof value === 'bigint') {
            return { $type: 'Integer', _value: value.toString() }
        } else if (isInt(value)) {
            return { $type: 'Integer', _value: value.toString() }
        } else if (value instanceof Uint8Array) {
            return { $type: 'Base64', _value: btoa(String.fromCharCode.apply(null, value)) }
        } else if (value instanceof Array) {
            return { $type: 'List', _value: value.map(this._encodeValue.bind(this)) }
        } else if (isIterable(value)) {
            return this._encodeValue(Array.from(value))
        } else if (isPoint(value)) {
            return {
                $type: 'Point', _value: value.z == null ?
                    `SRID=${int(value.srid).toString()};POINT (${value.x} ${value.y})` :
                    `SRID=${int(value.srid).toString()};POINT Z (${value.x} ${value.y} ${value.z})`
            }
        } else if (isDuration(value)) {
            return { $type: 'Duration', _value: value.toString() }
        } else if (isLocalTime(value)) {
            return { $type: 'LocalTime', _value: value.toString() }
        } else if (isTime(value)) {
            return { $type: 'Time', _value: value.toString() }
        } else if (isDate(value)) {
            return { $type: 'Date', _value: value.toString() }
        } else if (isLocalDateTime(value)) {
            return { $type: 'LocalDateTime', _value: value.toString() }
        } else if (isDateTime(value)) {
            if (value.timeZoneOffsetSeconds == null) {
                throw new Error(
                    'DateTime objects without "timeZoneOffsetSeconds" property ' +
                    'are prone to bugs related to ambiguous times. For instance, ' +
                    '2022-10-30T2:30:00[Europe/Berlin] could be GMT+1 or GMT+2.'
                )
            }
            
            if (value.timeZoneId != null) {
                return { $type: 'ZonedDateTime', _value: value.toString() }
            }
            return { $type: 'OffsetDateTime', _value: value.toString() }
        } else if (isVector(value)) {
            throw newError('Vectors are not supported yet on query api', error.PROTOCOL_ERROR)
        } else if (isRelationship(value) || isNode(value) || isPath(value) || isPathSegment(value) || isUnboundRelationship(value)) {
            throw newError('Graph types can not be ingested to the server', error.PROTOCOL_ERROR) 
        } else if (typeof value === 'object') {
            return { $type: "Map", _value: this._encodeParameters(value as Record<string, unknown>) }
        } else {
            throw newError(`Unable to convert parameter to http request. Value: ${value}`, error.PROTOCOL_ERROR)
        }
    }
}

function isIterable<T extends unknown = unknown>(obj: unknown): obj is Iterable<T> {
    if (obj == null) {
        return false
    }
    // @ts-expect-error
    return typeof obj[Symbol.iterator] === 'function'
}

function isSuccess(obj: RawQueryResponse): obj is RawQuerySuccessResponse {
    if (obj.errors != null) {
        return false
    }
    return true
}
