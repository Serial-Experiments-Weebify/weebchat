<script setup lang="ts">
import { ref } from 'vue';
import UserCreate from './components/user-create.vue';
import ChatView from './components/chat-view.vue';
import {Connect, CreateUser} from '../wailsjs/go/main/App';
import { chat } from '../wailsjs/go/models';

const isLoggedIn = ref(false);
const currentUser = ref<chat.User | null>(null);

interface LoginData {
  server: string;
  username: string;
}


async function handleLogin (data: LoginData) {
  await Connect(data.server);
  currentUser.value = await CreateUser(data.username)
  isLoggedIn.value = true;
};
</script>


<template>
  <UserCreate v-if="!isLoggedIn" @login="handleLogin" />
  <ChatView v-else :user="currentUser!" />
</template>

<style scoped>
  

</style>