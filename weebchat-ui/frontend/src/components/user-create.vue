<script setup lang="ts">
import { ref } from 'vue';
const server = ref('');
const username = ref('');
</script>

<template>
  <main id="login">
    <h1>Login</h1>
    <input v-model="server" type="text" placeholder="server address">
    <input v-model="username" type="text" placeholder="username">
    <button @click="$emit('login', { server, username })">Log in</button>
  </main>
</template>

