const {diag, DiagConsoleLogger, DiagLogLevel} = require( '@opentelemetry/api')
const {NodeSDK} = require( '@opentelemetry/sdk-node')
const {ConsoleSpanExporter } = require( '@opentelemetry/sdk-trace-node')
const {HttpInstrumentation} = require( '@opentelemetry/instrumentation-http')
const {ExpressLayerType} = require( "@opentelemetry/instrumentation-express")
const {ExpressInstrumentation} = require( '@opentelemetry/instrumentation-express')
const {BatchSpanProcessor} = require( "@opentelemetry/sdk-trace-base")
// import {OTLPTraceExporter} from '@opentelemetry/exporter-trace-otlp-grpc';

// Check node env. use debug level if development or error otherwise
const oTelLogLevel =
    process.env['NODE_ENV'] === 'development' ? DiagLogLevel.DEBUG : DiagLogLevel.ERROR

// For troubleshooting, set the log level to DiagLogLevel.DEBUG
diag.setLogger(new DiagConsoleLogger(), oTelLogLevel);

const sdk = new NodeSDK({
    // resource: TODO,
    instrumentations: [
        new HttpInstrumentation(),
        new ExpressInstrumentation({
            ignoreLayersType: [
                ExpressLayerType.MIDDLEWARE
            ]
        }),
    ],
    spanProcessors: [
        new BatchSpanProcessor(
            new ConsoleSpanExporter(),
            //new OTLPTraceExporter({
            //    url: "localhost:4317"
            //})
        )
    ]
})

sdk.start();
