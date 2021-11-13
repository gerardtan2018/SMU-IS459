from django.db import models

# Create your models here.

class User(models.Model):
    name = models.CharField(max_length=200)

    def __str__(self):
        return self.name

class Topic(models.Model):
    name = models.CharField(max_length = 200)

    my_post = models.ManyToManyField(
        User,
        through='Post',
        through_fields=('topic', 'user'))

    def __str__(self):
        return self.name

class Post(models.Model):
    user = models.ForeignKey(User, on_delete = models.CASCADE)
    topic = models.ForeignKey(Topic, on_delete = models.CASCADE)

    content = models.TextField()

class PostCount(models.Model):
    timestamp = models.DateTimeField(auto_now=False, auto_now_add=False)
    user_name = models.CharField(max_length=200)
    post_count = models.IntegerField()

    def __str__(self):
        return str(self.timestamp) + " - " + self.user_name + " : " + str(self.post_count)
