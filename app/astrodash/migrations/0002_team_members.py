from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("astrodash", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="TeamAffiliation",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=255)),
                ("order", models.PositiveIntegerField(default=0, help_text="Display order (lower first)")),
            ],
            options={
                "db_table": "astrodash_team_affiliations",
                "ordering": ["order", "name"],
            },
        ),
        migrations.CreateModel(
            name="TeamMember",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=255)),
                ("description", models.TextField(blank=True)),
                (
                    "image",
                    models.CharField(
                        blank=True,
                        help_text="Path under static/images/ (e.g. team/jane.jpg). Leave blank for no photo.",
                        max_length=512,
                    ),
                ),
                ("order", models.PositiveIntegerField(default=0, help_text="Display order within affiliation (lower first)")),
                (
                    "affiliation",
                    models.ForeignKey(
                        on_delete=models.CASCADE,
                        related_name="members",
                        to="astrodash.teamaffiliation",
                    ),
                ),
            ],
            options={
                "db_table": "astrodash_team_members",
                "ordering": ["affiliation", "order", "name"],
            },
        ),
    ]
