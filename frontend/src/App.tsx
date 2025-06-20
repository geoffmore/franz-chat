// create-react-app foo --template typescript

import React from 'react';
import './App.css';

function App() {
  return (
    <>
      <UI />
    </>
  );
}

function ChannelList() {
  return (
      <div className="channel-box">
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

export default App;
