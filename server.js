// server.js — Realtime quiz server (auto-progression, host = spectator)
const express = require('express');
const https = require('https');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');

const app = express();
const ssl = {
  key:  fs.readFileSync('/etc/letsencrypt/live/game.hansei.cam/privkey.pem'),
  cert: fs.readFileSync('/etc/letsencrypt/live/game.hansei.cam/fullchain.pem'),
};
const server = https.createServer(ssl, app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

const quizzes = JSON.parse(fs.readFileSync(path.join(__dirname, 'quizzes.json'), 'utf-8'));

// =============================================================
// 게임 상태
// state: 'lobby' | 'question' | 'answer_reveal' | 'ranking' | 'ended'
// =============================================================
const rooms = new Map();

const ANSWER_REVEAL_SEC = 3;   // 정답만 크게 보여주는 시간
const RANKING_SEC = 5;          // 랭킹 보여주는 시간

function generatePin() {
  let pin;
  do {
    pin = String(Math.floor(100000 + Math.random() * 900000));
  } while (rooms.has(pin));
  return pin;
}

function publicPlayers(room) {
  return [...room.players.values()].map(p => ({ nick: p.nick, score: p.score }));
}

function leaderboard(room) {
  return publicPlayers(room).sort((a, b) => b.score - a.score);
}

function clearRoomTimers(room) {
  if (room.timer) { clearTimeout(room.timer); room.timer = null; }
  if (room.tickInterval) { clearInterval(room.tickInterval); room.tickInterval = null; }
}

// =============================================================
// HTTP
// =============================================================
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.get('/host', (req, res) => res.sendFile(path.join(__dirname, 'public', 'host.html')));
app.get('/play', (req, res) => res.sendFile(path.join(__dirname, 'public', 'play.html')));

app.post('/api/rooms', (req, res) => {
  const quizId = req.body.quizId || 'default';
  const quiz = quizzes[quizId];
  if (!quiz) return res.status(404).json({ error: 'quiz not found' });

  const pin = generatePin();
  rooms.set(pin, {
    hostId: null,
    quiz,
    currentQ: -1,
    players: new Map(),
    state: 'lobby',
    questionStartTs: 0,
    timer: null,
    tickInterval: null
  });
  res.json({ pin, title: quiz.title, total: quiz.questions.length });
});

// =============================================================
// Socket.IO
// =============================================================
io.on('connection', (socket) => {

  socket.on('host:join', ({ pin }) => {
    const room = rooms.get(pin);
    if (!room) return socket.emit('error:msg', '존재하지 않는 PIN입니다');
    room.hostId = socket.id;
    socket.join(pin);
    socket.data = { role: 'host', pin };
    socket.emit('host:joined', { pin, players: publicPlayers(room) });
  });

  socket.on('player:join', ({ pin, nick }) => {
    const room = rooms.get(pin);
    if (!room) return socket.emit('error:msg', '존재하지 않는 PIN입니다');
    if (room.state !== 'lobby') return socket.emit('error:msg', '이미 시작된 게임입니다');
    nick = (nick || '').trim().slice(0, 16);
    if (!nick) return socket.emit('error:msg', '닉네임을 입력해주세요');
    if ([...room.players.values()].some(p => p.nick === nick))
      return socket.emit('error:msg', '이미 사용 중인 닉네임입니다');

    room.players.set(socket.id, { nick, score: 0, answered: false, lastDelta: 0, lastCorrect: false });
    socket.join(pin);
    socket.data = { role: 'player', pin, nick };

    socket.emit('player:joined', { pin, nick });
    io.to(pin).emit('lobby:players', publicPlayers(room));
    if (room.hostId) io.to(room.hostId).emit('host:players', publicPlayers(room));
  });

  // 호스트는 시작 버튼만 누름 (이후 자동 진행)
  socket.on('host:start', () => {
    const { pin } = socket.data || {};
    const room = rooms.get(pin);
    if (!room || socket.id !== room.hostId) return;
    if (room.state !== 'lobby') return;
    if (room.players.size === 0) return socket.emit('error:msg', '참가자가 없습니다');
    countdown(pin);
  });

  socket.on('player:answer', ({ choice }) => {
    const { pin } = socket.data || {};
    const room = rooms.get(pin);
    if (!room || room.state !== 'question') return;
    const player = room.players.get(socket.id);
    if (!player || player.answered) return;

    const q = room.quiz.questions[room.currentQ];
    const elapsed = (Date.now() - room.questionStartTs) / 1000;
    const correct = choice === q.answer;

    let delta = 0;
    if (correct) {
      const ratio = Math.max(0, 1 - elapsed / q.timeLimit);
      delta = Math.round(500 + 500 * ratio);
    }
    player.score += delta;
    player.lastDelta = delta;
    player.lastCorrect = correct;
    player.answered = true;

    socket.emit('player:answered', { correct, delta, score: player.score });

    const answered = [...room.players.values()].filter(p => p.answered).length;
    if (answered === room.players.size) {
      clearRoomTimers(room);
      revealAnswer(pin);
    } else if (room.hostId) {
      io.to(room.hostId).emit('host:progress', { answered, total: room.players.size });
    }
  });

  socket.on('disconnect', () => {
    const { role, pin } = socket.data || {};
    if (!pin) return;
    const room = rooms.get(pin);
    if (!room) return;

    if (role === 'host') {
      clearRoomTimers(room);
      io.to(pin).emit('error:msg', '호스트가 종료했습니다');
      rooms.delete(pin);
    } else if (role === 'player') {
      room.players.delete(socket.id);
      io.to(pin).emit('lobby:players', publicPlayers(room));
      if (room.hostId) io.to(room.hostId).emit('host:players', publicPlayers(room));
    }
  });
});

// =============================================================
// 게임 진행 (자동)
// =============================================================

function countdown(pin) {
  const room = rooms.get(pin);
  if (!room) return;
  room.state = 'countdown';
  let count = 3;
  io.to(pin).emit('countdown:tick', { count });
  room.tickInterval = setInterval(() => {
    count -= 1;
    if (count > 0) {
      io.to(pin).emit('countdown:tick', { count });
    } else {
      clearRoomTimers(room);
      startQuestion(pin);
    }
  }, 1000);
}

function startQuestion(pin) {
  const room = rooms.get(pin);
  if (!room) return;

  room.currentQ += 1;
  if (room.currentQ >= room.quiz.questions.length) {
    return endGame(pin);
  }

  const q = room.quiz.questions[room.currentQ];
  room.state = 'question';
  room.questionStartTs = Date.now();
  for (const p of room.players.values()) p.answered = false;

  io.to(pin).emit('question:show', {
    index: room.currentQ,
    total: room.quiz.questions.length,
    q: q.q,
    choices: q.choices,
    timeLimit: q.timeLimit
  });

  // 매초 카운트다운 broadcast
  let remaining = q.timeLimit;
  io.to(pin).emit('tick', { remaining });
  room.tickInterval = setInterval(() => {
    remaining -= 1;
    if (remaining >= 0) io.to(pin).emit('tick', { remaining });
  }, 1000);

  clearTimeout(room.timer);
  room.timer = setTimeout(() => revealAnswer(pin), q.timeLimit * 1000);
}

function revealAnswer(pin) {
  const room = rooms.get(pin);
  if (!room || room.state !== 'question') return;
  clearRoomTimers(room);
  room.state = 'answer_reveal';

  const q = room.quiz.questions[room.currentQ];
  io.to(pin).emit('answer:reveal', {
    answer: q.answer,
    answerText: q.choices[q.answer]
  });

  room.timer = setTimeout(() => showRanking(pin), ANSWER_REVEAL_SEC * 1000);
}

function showRanking(pin) {
  const room = rooms.get(pin);
  if (!room || room.state !== 'answer_reveal') return;
  room.state = 'ranking';

  const board = leaderboard(room);
  const isLast = room.currentQ + 1 >= room.quiz.questions.length;

  io.to(pin).emit('ranking:show', { leaderboard: board, isLast });

  room.timer = setTimeout(() => {
    if (isLast) endGame(pin);
    else startQuestion(pin);
  }, RANKING_SEC * 1000);
}

function endGame(pin) {
  const room = rooms.get(pin);
  if (!room) return;
  clearRoomTimers(room);
  room.state = 'ended';
  io.to(pin).emit('game:ended', { leaderboard: leaderboard(room) });
}

const PORT = process.env.PORT || 443;
server.listen(PORT, () => {
  console.log(`✅ QuizN clone running on https://game.hansei.cam`);
  console.log(`   호스트:   https://game.hansei.cam/host`);
  console.log(`   플레이어: https://game.hansei.cam/`);
});
