
if (true) {
    require("./tracing")
}
const express = require('express')
const app = express()
const port = 3000
// TODO - env var for port
// TODO - env var for backend port

app.get('/', (_req, res) => {
    res.send('Hello World!')
})

app.listen(port, () => {
    console.log(`Example app listening on port ${port}`)
})
