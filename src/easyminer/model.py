from typing import override

from tortoise import fields
from tortoise.models import Model


class Tournament(Model):
    id = fields.IntField(primary_key=True)
    name = fields.TextField()

    @override
    def __str__(self):
        return self.name


class Event(Model):
    id = fields.IntField(primary_key=True)
    name = fields.TextField()
    tournament: fields.ForeignKeyRelation[Tournament] = fields.ForeignKeyField(
        "models.Tournament", related_name="events"
    )
    participants = fields.ManyToManyField(
        "models.Team", related_name="events", through="event_team"
    )

    @override
    def __str__(self):
        return self.name


class Team(Model):
    id = fields.IntField(primary_key=True)
    name = fields.TextField()

    @override
    def __str__(self):
        return self.name
