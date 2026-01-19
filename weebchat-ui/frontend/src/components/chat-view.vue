<script setup lang="ts">
import { onMounted, ref } from 'vue';
import { EventsOn } from '../../wailsjs/runtime/runtime'
import { FetchTopics, CreateTopic, LikeMessage, PostMessage, StartSubscription } from '../../wailsjs/go/main/App'
import { chat } from '../../wailsjs/go/models';

const props = defineProps<{
  user: chat.User
}>()

const selectedTopic = ref(0)
const topics = ref<chat.Topic[]>([]);
const newTopicName = ref('');
const newMessage = ref('');
const messages = ref<chat.Message[]>([]);

async function TopicCreate() {
  if (newTopicName.value) {
    topics.value.push(await CreateTopic(newTopicName.value))
    newTopicName.value = '';
  }
};

function sendMessage() {
  if (newMessage.value) {
    PostMessage(selectedTopic.value, props.user.id!, newMessage.value)
    newMessage.value = '';
  }
};

function likeMessage(msg: chat.Message) {
  LikeMessage(msg.topic_id!, msg.id!, msg.user_id!)
};

async function refreshTopics() {
    try {
      const fT = await FetchTopics();
      if(fT.length !== 0)
        topics.value = fT
    } catch (err) {
      console.error("Failed to fetch topics:", err)
    }
}

async function Subscribe(tid: number) {
  if (selectedTopic.value === tid) return;
  selectedTopic.value = tid; 
  StartSubscription(props.user.id!, tid);
  messages.value = []
}

onMounted(() => {
    EventsOn("message_event", (data: any) => {
        const msg = chat.Message.createFrom(data)
        const existingMsg = messages.value.find(m => m.id === msg.id);
        if (existingMsg) {
          existingMsg.likes = msg.likes;
        }
        else if(msg.topic_id == selectedTopic.value) {
          messages.value.push(msg)
        }
    })
})

</script>


<template>
  <main id="main">
    <aside>
      <h2>Weebchat</h2>
      
      <span class="topicsheader">
        Topics <button @click="refreshTopics">🔃</button>
      </span>

      <div class="topics">
        <div 
          v-for="topic in topics" 
          :key="topic.id"
          :class="['topic', { active: selectedTopic === topic.id }]"
          @click="Subscribe(topic.id!)"
        >
          {{ topic.name }}
        </div>
      </div>

      <div class="newtopic">
        <input v-model="newTopicName" type="text">
        <button @click="TopicCreate">Create</button>
      </div>
    </aside>
    
    <div id="content">
      <div class="messages">
        <div v-for="msg in messages" :key="msg.id" class="message">
          <span class="username">User#{{ msg.user_id ?? '?' }}:</span>
          <p class="text">{{ msg.text }}</p>
          <button @click="likeMessage(msg)"> ❤️ {{ msg.likes }} </button>
        </div>
      </div>

      <div class="send">
        <input v-model="newMessage" type="text" @keyup.enter="sendMessage">
        <button @click="sendMessage">Send</button>
      </div>
    </div>
  </main>
</template>

