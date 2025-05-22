import flet as ft
from src.config import APP_NAME

def build_home_view(page: ft.Page):
    features = [
        ft.Container(
            content=ft.Column([
                ft.Icon("search", size=50),
                ft.Text("Channel Discovery", theme_style=ft.TextThemeStyle.HEADLINE_SMALL),
                ft.Text("Find YouTube channels by keywords.")
            ]),
            alignment=ft.alignment.center,
            padding=20,
            border_radius=10,
            width=200,
        ),
        ft.Container(
            content=ft.Column([
                ft.Icon("subtitles", size=50),
                ft.Text("Subtitle Processing", theme_style=ft.TextThemeStyle.HEADLINE_SMALL),
                ft.Text("Grab, fix, and translate subtitles.")
            ]),
            alignment=ft.alignment.center,
            padding=20,
            border_radius=10,
            width=200,
        ),
        ft.Container(
            content=ft.Column([
                ft.Icon("model_training", size=50),
                ft.Text("AI Analysis", theme_style=ft.TextThemeStyle.HEADLINE_SMALL),
                ft.Text("Summarize content, identify themes.")
            ]),
            alignment=ft.alignment.center,
            padding=20,
            border_radius=10,
            width=200,
        ),
         ft.Container(
            content=ft.Column([
                ft.Icon("table_chart", size=50),
                ft.Text("Data Export", theme_style=ft.TextThemeStyle.HEADLINE_SMALL),
                ft.Text("Export your findings to CSV.")
            ]),
            alignment=ft.alignment.center,
            padding=20,
            border_radius=10,
            width=200,
        )
    ]

    feature_display = ft.Row(
        controls=features,
        scroll=ft.ScrollMode.AUTO,
        spacing=20,
        vertical_alignment=ft.CrossAxisAlignment.START
    )

    return ft.Column(
        [
            ft.Text(APP_NAME, theme_style=ft.TextThemeStyle.DISPLAY_MEDIUM, weight=ft.FontWeight.BOLD),
            ft.Text(
                "I've watched C-beams glitter in the dark, near the Tannhauser gate.",
                theme_style=ft.TextThemeStyle.HEADLINE_SMALL,
            ),
            ft.Divider(height=20, color="transparent"),
            ft.Text("Key Features:", theme_style=ft.TextThemeStyle.TITLE_LARGE),
            feature_display,
            ft.Divider(height=20, color="transparent"),
            ft.Text("Get started by creating or opening a database from the sidebar.", theme_style=ft.TextThemeStyle.BODY_LARGE)
        ],
        alignment=ft.MainAxisAlignment.START,
        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        spacing=15,
        scroll=ft.ScrollMode.ADAPTIVE,
    ) 