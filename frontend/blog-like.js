const blogLikeMixin = {
  data() {
    return { likePending: {} };
  },
  methods: {
    async toggleBlogLike(blog) {
      if (!blog.id || this.likePending[blog.id]) {
        return false;
      }
      const id = blog.id;
      const desired = !blog.isLike;
      const url = "/blog/like/" + id;
      this.$set(this.likePending, id, true);
      try {
        if (desired) {
          await axios.put(url + "?liked=true");
        } else {
          await axios.delete(url);
        }
        blog.isLike = desired;
        blog.liked = Math.max(0, blog.liked + (desired ? 1 : -1));
        return true;
      } catch (error) {
        this.$message.error(error);
        return false;
      } finally {
        this.$delete(this.likePending, id);
      }
    }
  }
};
