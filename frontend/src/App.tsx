// create-react-app foo --template typescript

import React from 'react';
import './App.css';
import {useState, useEffect } from 'react';

// TODO - api import

function App() {
  return (
    <>
      <UI />
    </>
  );
}

// TODO - determine if this function needs to be async
function ChannelList() {
    const [channels, setChannels] = useState([]);
    useEffect(() => {
        const channels = getChannels(); // getChannels returns a promise, which is not very useful
        console.log(channels);
        // Empty cleanup function
        return () => {};
    }, [channels])
    //}) // TODO - figure out how to pass the channelList to the props

  return (
      <div className="channel-box">
          {channels}
      </div>
  )
}

function ChatLog() {
  return (
      <div className="chat-log"></div>
  )
}

function LoginBox() {
  return (
      <div className="login-box"></div>
  )
}

function ChatBox() {
  return (
      <div className="chat-box"></div>
  )
}

function UI() {
    return (
        <div className="container">
            <ChannelList />
            <LoginBox/>
            <ChatLog/>
            <ChatBox/>
        </div>
    )
}

// TODO - websockets
// TODO - API calls to BE

// useEffect hook for API calls
// store data inside state hooks

async function getChannels(): Promise<string> {
    let data;
    try {
        const response = await fetch('http://localhost:8008/channels', {
            method: 'GET',
            // mode: 'no-cors', // NOTE - no-cors mode and https://stackoverflow.com/questions/36840396/
            headers: {
                "Accept": "application/json",
                // "Access-Control-Allow-Origin": "*",
            }
        });
        data = await response.json();
    } catch (err) {
        console.error(err);
    }
    return data;
}

export default App;
