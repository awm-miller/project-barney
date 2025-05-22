import flet as ft

def build_pipeline_intro_view(page: ft.Page):
    print("DEBUG: Building Pipeline Intro View")
    def on_get_started_click(e):
        print("DEBUG: Pipeline Intro - Get Started clicked. Navigating to /pipeline_db_setup")
        page.go("/pipeline_db_setup")

    return ft.Column(
        [
            ft.Text("Pipeline Wizard - Stage 1: Database Creation", theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM, weight=ft.FontWeight.BOLD),
            ft.Text(
                "This wizard will guide you through creating a new database and populating it with videos from a YouTube playlist.",
                text_align=ft.TextAlign.CENTER
            ),
            ft.Container(height=10),
            ft.Text(
                "After the database is created and subtitles are fetched, you will be taken to a completion page.",
                text_align=ft.TextAlign.CENTER, size=12, italic=True
            ),
            ft.Container(height=20),
            ft.Column(
                [
                    ft.ListTile(
                        leading=ft.Icon("create_new_folder", color="primarycontainer"),
                        title=ft.Text("Step 1: Database Details"),
                        subtitle=ft.Text("Provide a name for your database."),
                    ),
                    ft.ListTile(
                        leading=ft.Icon("playlist_add", color="primarycontainer"),
                        title=ft.Text("Step 2: Playlist Information"),
                        subtitle=ft.Text("Specify the YouTube playlist URL and an optional API key."),
                    ),
                    ft.ListTile(
                        leading=ft.Icon("sync", color="primarycontainer"), # Changed icon
                        title=ft.Text("Step 3: Run Process"),
                        subtitle=ft.Text("The script will create the database and fetch subtitles."),
                    ),
                     ft.ListTile(
                        leading=ft.Icon("check_circle_outline", color="primarycontainer"), 
                        title=ft.Text("Step 4: Completion"),
                        subtitle=ft.Text("View the created database or return home."),
                    ),
                ],
                spacing=5,
                alignment=ft.MainAxisAlignment.CENTER,
            ),
            ft.Container(height=20),
            ft.Row(
                [
                    ft.FilledButton(
                        "Get Started",
                        icon="arrow_forward",
                        on_click=on_get_started_click
                    )
                ],
                alignment=ft.MainAxisAlignment.CENTER
            )
        ],
        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        alignment=ft.MainAxisAlignment.CENTER,
        spacing=15,
        expand=True,
        scroll=ft.ScrollMode.ADAPTIVE,
    ) 