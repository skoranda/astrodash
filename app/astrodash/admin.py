from django.contrib import admin
from astrodash.models import TeamAffiliation, TeamMember


class TeamMemberInline(admin.TabularInline):
    model = TeamMember
    extra = 0
    ordering = ["order", "name"]


@admin.register(TeamAffiliation)
class TeamAffiliationAdmin(admin.ModelAdmin):
    list_display = ["name", "order"]
    ordering = ["order", "name"]
    inlines = [TeamMemberInline]


@admin.register(TeamMember)
class TeamMemberAdmin(admin.ModelAdmin):
    list_display = ["name", "affiliation", "order"]
    list_filter = ["affiliation"]
    ordering = ["affiliation", "order", "name"]
