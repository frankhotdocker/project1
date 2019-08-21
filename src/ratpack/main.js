const app = new Vue({
    el: "#app",
    data: {
      editFriend: null,
      friends: [],
    },
    methods: {
      deleteFriend(id, i) {
        fetch("http://localhost:5050/nested/" + id, {
          method: "GET"
        })
        .then(() => {
          this.friends.splice(i, 1);
        })
      },
      updateFriend(friend) {
        fetch("http://localhost:5050/nested/" + friend.id, {
          body: JSON.stringify(friend),
          method: "POST",
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
      fetch("http://localhost:5050/cassandra")
        .then(response => response.json())
        .then((data) => {
           this.friends = data;
        })
    },
    template: `
    <div>
      <li v-for="friend, i in friends">
        <div v-if="editFriend === friend.dfp_token">
          <input v-on:keyup.13="updateFriend(friend)" v-model="friend.account_holder_first_name" />
          <button v-on:click="updateFriend(friend)">save</button>
        </div>
        <div v-else>
          <button v-on:click="editFriend = friend.dfp_token">edit</button>
          <button v-on:click="deleteFriend(friend.dfp_token, i)">x</button>
          {{friend.dfp_token}}
        </div>
      </li>
    </div>
    `,
});