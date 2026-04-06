const DEFAULT_SERVER = "http://localhost:8000";
const MESSAGE_POLL_MS = 2500;
const STATUS_POLL_MS = 2500;

const elements = {
  usernameInput: document.getElementById("usernameInput"),
  serverSelect: document.getElementById("serverSelect"),
  recipientInput: document.getElementById("recipientInput"),
  messageInput: document.getElementById("messageInput"),
  saveProfileButton: document.getElementById("saveProfileButton"),
  sendButton: document.getElementById("sendButton"),
  refreshButton: document.getElementById("refreshButton"),
  chatAlert: document.getElementById("chatAlert"),
  messagesList: document.getElementById("messagesList"),
  messageCountLabel: document.getElementById("messageCountLabel"),
  connectionState: document.getElementById("connectionState"),
  activeServerLabel: document.getElementById("activeServerLabel"),
  activeLeaderLabel: document.getElementById("activeLeaderLabel"),
  connectedNodeValue: document.getElementById("connectedNodeValue"),
  leaderValue: document.getElementById("leaderValue"),
  raftRoleValue: document.getElementById("raftRoleValue"),
  raftTermValue: document.getElementById("raftTermValue"),
  commitIndexValue: document.getElementById("commitIndexValue"),
  clockOffsetValue: document.getElementById("clockOffsetValue"),
  knownNodesValue: document.getElementById("knownNodesValue"),
  replicasValue: document.getElementById("replicasValue"),
  syncStatusValue: document.getElementById("syncStatusValue"),
  lastSyncValue: document.getElementById("lastSyncValue"),
  bestRttValue: document.getElementById("bestRttValue"),
  manualSkewValue: document.getElementById("manualSkewValue"),
};

let messageTimer = null;
let statusTimer = null;

function getStoredProfile() {
  return {
    username: localStorage.getItem("chat_ui_username") || "",
    serverUrl: localStorage.getItem("chat_ui_server") || DEFAULT_SERVER,
    recipient: localStorage.getItem("chat_ui_recipient") || "all",
  };
}

function saveProfile() {
  localStorage.setItem("chat_ui_username", elements.usernameInput.value.trim());
  localStorage.setItem("chat_ui_server", elements.serverSelect.value);
  localStorage.setItem("chat_ui_recipient", elements.recipientInput.value.trim() || "all");
  showAlert("Profile saved for this browser session.", false);
  refreshAll();
}

function loadProfile() {
  const profile = getStoredProfile();
  elements.usernameInput.value = profile.username;
  elements.serverSelect.value = profile.serverUrl;
  elements.recipientInput.value = profile.recipient;
}

function getActiveServer() {
  return elements.serverSelect.value || DEFAULT_SERVER;
}

function getRecipient() {
  return elements.recipientInput.value.trim() || "all";
}

function showAlert(message, isError) {
  elements.chatAlert.hidden = false;
  elements.chatAlert.textContent = message;
  elements.chatAlert.style.background = isError ? "var(--warn-soft)" : "var(--accent-soft)";
  elements.chatAlert.style.color = isError ? "var(--warn)" : "var(--accent)";
}

function hideAlert() {
  elements.chatAlert.hidden = true;
}

function setConnectionState(text, isWarning = false) {
  elements.connectionState.textContent = text;
  elements.connectionState.classList.toggle("warning", isWarning);
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `Request failed with status ${response.status}`);
  }
  return response.json();
}

function renderMessages(messages, username) {
  elements.messagesList.innerHTML = "";
  elements.messageCountLabel.textContent = `${messages.length} message${messages.length === 1 ? "" : "s"}`;

  if (!messages.length) {
    const empty = document.createElement("div");
    empty.className = "message-card";
    empty.innerHTML = "<p class='message-body'>No messages yet for this view.</p>";
    elements.messagesList.appendChild(empty);
    return;
  }

  messages.forEach((message) => {
    const card = document.createElement("article");
    card.className = "message-card";
    const targetLabel = message.receiver === "all" ? "broadcast" : `to ${message.receiver}`;
    const fromLabel = message.sender === username ? "You" : message.sender;
    card.innerHTML = `
      <div class="message-meta">
        <span><strong>${fromLabel}</strong> ${targetLabel}</span>
        <span>${message.corrected_timestamp || message.timestamp}</span>
      </div>
      <p class="message-body">${message.content}</p>
      <div class="message-meta">
        <span>logical ${message.logical_timestamp}</span>
        <span>${message.id.slice(0, 8)}...</span>
      </div>
    `;
    elements.messagesList.appendChild(card);
  });
}

function renderStatus(status) {
  const { server, time, raft } = status;
  elements.activeServerLabel.textContent = `Active server: ${getActiveServer()}`;
  elements.activeLeaderLabel.textContent = `Leader: ${server.current_primary_url}`;
  elements.connectedNodeValue.textContent = server.own_url;
  elements.leaderValue.textContent = server.current_primary_url;
  elements.raftRoleValue.textContent = raft.role;
  elements.raftTermValue.textContent = String(raft.term);
  elements.commitIndexValue.textContent = String(raft.commit_index);
  elements.clockOffsetValue.textContent = `${time.clock_offset_ms} ms`;
  elements.knownNodesValue.textContent = server.known_nodes.join(", ");
  elements.replicasValue.textContent = (server.replicas || []).join(", ") || "--";
  elements.syncStatusValue.textContent = time.sync_status || "--";
  elements.lastSyncValue.textContent = time.last_sync_at || "--";
  elements.bestRttValue.textContent = time.best_sync_rtt_ms == null ? "--" : `${time.best_sync_rtt_ms} ms`;
  elements.manualSkewValue.textContent = `${time.manual_clock_skew_ms} ms`;
}

async function refreshMessages() {
  const username = elements.usernameInput.value.trim();
  const serverUrl = getActiveServer();
  const query = username ? `?receiver=${encodeURIComponent(username)}` : "";
  const messages = await fetchJson(`${serverUrl}/messages${query}`);
  renderMessages(messages, username);
}

async function refreshStatus() {
  const serverUrl = getActiveServer();
  const status = await fetchJson(`${serverUrl}/ui/status`);
  renderStatus(status);
}

async function refreshAll() {
  try {
    setConnectionState("Connected");
    hideAlert();
    await Promise.all([refreshMessages(), refreshStatus()]);
  } catch (error) {
    setConnectionState("Server unavailable", true);
    showAlert(`Could not reach ${getActiveServer()}. Switch servers or restart the node.`, true);
    console.error(error);
  }
}

async function sendMessage() {
  const sender = elements.usernameInput.value.trim();
  const receiver = getRecipient();
  const content = elements.messageInput.value.trim();

  if (!sender) {
    showAlert("Enter your name before sending messages.", true);
    return;
  }

  if (!content) {
    showAlert("Type a message before sending.", true);
    return;
  }

  try {
    await fetchJson(`${getActiveServer()}/send`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sender, receiver, content }),
    });
    elements.messageInput.value = "";
    hideAlert();
    refreshAll();
  } catch (error) {
    showAlert(`Send failed: ${error.message}`, true);
  }
}

function startPolling() {
  if (messageTimer) {
    clearInterval(messageTimer);
  }
  if (statusTimer) {
    clearInterval(statusTimer);
  }

  messageTimer = setInterval(() => {
    refreshMessages().catch((error) => {
      setConnectionState("Server unavailable", true);
      console.error(error);
    });
  }, MESSAGE_POLL_MS);

  statusTimer = setInterval(() => {
    refreshStatus().catch((error) => {
      setConnectionState("Server unavailable", true);
      console.error(error);
    });
  }, STATUS_POLL_MS);
}

function bindEvents() {
  elements.saveProfileButton.addEventListener("click", saveProfile);
  elements.sendButton.addEventListener("click", sendMessage);
  elements.refreshButton.addEventListener("click", refreshAll);
  elements.serverSelect.addEventListener("change", () => {
    localStorage.setItem("chat_ui_server", getActiveServer());
    refreshAll();
  });
  elements.usernameInput.addEventListener("change", () => {
    localStorage.setItem("chat_ui_username", elements.usernameInput.value.trim());
    refreshAll();
  });
  elements.recipientInput.addEventListener("change", () => {
    localStorage.setItem("chat_ui_recipient", getRecipient());
    refreshAll();
  });
  elements.messageInput.addEventListener("keydown", (event) => {
    if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
      sendMessage();
    }
  });
}

function init() {
  loadProfile();
  bindEvents();
  refreshAll();
  startPolling();
}

init();
