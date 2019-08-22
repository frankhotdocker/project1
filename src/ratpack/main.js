const app = new Vue({
    el: "#app",
    data: {
      editFriend: null,
      friends: [],
    },
    methods: {
      deleteFriend(id, i) {
        fetch("https://jsonplaceholder.typicode.com/posts/" + id, {
          method: "DELETE"
        })
        .then(() => {
          this.friends.splice(i, 1);
        })
      },
      updateFriend(friend) {
        fetch("https://jsonplaceholder.typicode.com/posts/" + friend.id, {
          body: JSON.stringify(friend),
          method: "PUT",
          headers: {
            "Content-Type": "application/json",
          },
        })
        .then(() => {
          this.editFriend = null;
        })
      }
    },
    mounted() {
      fetch("https://jsonplaceholder.typicode.com/posts")
        .then(response => response.json())
        .then((data) => {
           this.friends = data;
        })
    },
    template: `
    <div>
      <li v-for="friend, i in friends">
        <div v-if="editFriend === friend.userId">
          <input v-on:keyup.enter="updateFriend(friend)" v-model="friend.title" />
          <button v-on:click="updateFriend(friend)">save</button>
        </div>
        <div v-else>
          <button v-on:click="editFriend = friend.userId">edit</button>
          <button v-on:click="deleteFriend(friend.userId, i)">x</button>
          {{friend.id}}<br/>{{friend.userId}}<br/>{{friend.title}}
        </div>
      </li>
    </div>
    `,
});