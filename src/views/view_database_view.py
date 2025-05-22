import flet as ft
from typing import Dict, Optional

from src.utils.db_utils import fetch_videos_for_view
from src.utils.ui_utils import show_video_details_dialog, check_active_db_and_show_snackbar
from src.config import ITEMS_PER_PAGE_VIEW_DB

def build_view_database_view(page: ft.Page):
    if not check_active_db_and_show_snackbar(page):
        return ft.Column([ft.Text("Please select a database to view its content.", theme_style=ft.TextThemeStyle.TITLE_LARGE, text_align=ft.TextAlign.CENTER)], horizontal_alignment=ft.CrossAxisAlignment.CENTER, alignment=ft.MainAxisAlignment.CENTER, expand=True)

    search_field = ft.TextField(
        label="Search by title, video ID, or channel ID...", 
        hint_text="Enter keywords and press Enter or click Search", 
        width=500, 
        # on_submit will be set later where perform_search is defined
    )
    
    datatable_columns = [
        ft.DataColumn(ft.Text("Video ID (Clickable)")),
        ft.DataColumn(ft.Text("Title")),
        ft.DataColumn(ft.Text("Published")),
        ft.DataColumn(ft.Text("Actions")),
    ]

    data_table = ft.DataTable(
        ref=ft.Ref[ft.DataTable](), # Assign ref directly here
        columns=datatable_columns,
        rows=[],
        column_spacing=10,
        divider_thickness=0.5,
        expand=True,
    )
    data_table_ref = data_table.ref # Get the ref for later use

    current_db_page_val = 1
    total_db_videos_val = 0
    db_page_size_val = ITEMS_PER_PAGE_VIEW_DB

    page_info_text = ft.Text(f"Page {current_db_page_val} of 1")
    prev_button = ft.IconButton(icon="arrow_back", disabled=True)
    next_button = ft.IconButton(icon="arrow_forward", disabled=True)
    
    pagination_controls_row = ft.Row(
        [prev_button, page_info_text, next_button],
        alignment=ft.MainAxisAlignment.CENTER,
        visible=False 
    )

    def update_pagination_controls():
        nonlocal total_db_videos_val, current_db_page_val, db_page_size_val
        total_pages = (total_db_videos_val + db_page_size_val - 1) // db_page_size_val if db_page_size_val > 0 else 1
        total_pages = max(1, total_pages)

        page_info_text.value = f"Page {current_db_page_val} of {total_pages}"
        prev_button.disabled = current_db_page_val <= 1
        next_button.disabled = current_db_page_val >= total_pages
        
        pagination_controls_row.visible = total_db_videos_val > 0
        if page.client_storage: # Ensure page updates are safe
            pagination_controls_row.update()
            page_info_text.update()
            prev_button.update()
            next_button.update()

    def update_data_table(data: Dict):
        nonlocal total_db_videos_val 
        videos = data.get("videos", [])
        total_db_videos_val = data.get("total_count", 0)
        
        if data_table_ref.current:
            data_table_ref.current.rows.clear()
            if videos:
                for video_data in videos:
                    video_id = video_data.get('video_id', 'N/A')
                    video_url = f"https://www.youtube.com/watch?v={video_id}"
                    cells = [
                        ft.DataCell(
                            ft.Text(video_id, color="lightblue", overflow=ft.TextOverflow.ELLIPSIS), 
                            on_tap=lambda _, url=video_url: page.launch_url(url)
                        ),
                        ft.DataCell(ft.Text(video_data.get('title', 'N/A'), overflow=ft.TextOverflow.ELLIPSIS)),
                        ft.DataCell(ft.Text(str(video_data.get('published_at', 'N/A')).split(' ')[0] if video_data.get('published_at') else 'N/A')),
                        ft.DataCell(ft.IconButton(icon="visibility", tooltip="View Details", on_click=lambda _, vd=video_data: show_video_details_dialog(page, vd)))
                    ]
                    data_table_ref.current.rows.append(ft.DataRow(cells=cells))
            if page.client_storage: data_table_ref.current.update() # Ensure page updates are safe
        update_pagination_controls()

    def _fetch_and_update_page_data(page_num_to_load: int, search_term_val: Optional[str]):
        nonlocal current_db_page_val
        if hasattr(page, 'active_db_path') and page.active_db_path:
            current_db_page_val = page_num_to_load
            fetched_data = fetch_videos_for_view(
                str(page.active_db_path), 
                search_term_val,
                page_number=current_db_page_val,
                page_size=db_page_size_val
            )
            update_data_table(fetched_data)
        else:
            update_data_table({"videos": [], "total_count": 0})

    def perform_search(e=None, is_new_search: bool = False):
        nonlocal current_db_page_val 
        if is_new_search:
            current_db_page_val = 1
        _fetch_and_update_page_data(current_db_page_val, search_field.value)
    
    search_field.on_submit = lambda e: perform_search(e, is_new_search=True)

    def initial_load_threaded(): # Renamed to indicate it's for threading
        nonlocal current_db_page_val
        current_db_page_val = 1
        _fetch_and_update_page_data(1, search_field.value)

    def go_to_next_page(e):
        nonlocal total_db_videos_val, current_db_page_val, db_page_size_val 
        total_pages = (total_db_videos_val + db_page_size_val - 1) // db_page_size_val
        if current_db_page_val < total_pages:
            _fetch_and_update_page_data(current_db_page_val + 1, search_field.value)

    def go_to_prev_page(e):
        nonlocal current_db_page_val
        if current_db_page_val > 1:
            _fetch_and_update_page_data(current_db_page_val - 1, search_field.value)
            
    prev_button.on_click = go_to_prev_page
    next_button.on_click = go_to_next_page
    
    view_layout = ft.Column(
        [
            ft.Text("View Database Content", theme_style=ft.TextThemeStyle.HEADLINE_MEDIUM),
            ft.Row([search_field, ft.ElevatedButton("Search", icon="search", on_click=lambda e: perform_search(e, is_new_search=True))], alignment=ft.MainAxisAlignment.START),
            ft.Divider(),
            ft.Row([data_table], scroll=ft.ScrollMode.ADAPTIVE, expand=True), 
            pagination_controls_row 
        ],
        expand=True, spacing=10, 
        horizontal_alignment=ft.CrossAxisAlignment.STRETCH
    )
    
    if hasattr(page, 'active_db_path') and page.active_db_path:
        page.run_thread(initial_load_threaded) # Run initial load in a thread
    else: 
        if data_table_ref.current:
            data_table_ref.current.rows = [
                ft.DataRow(cells=[
                    ft.DataCell(ft.Text("No database is currently active. Please select one."), col_span=len(datatable_columns))
                ])
            ]
        update_pagination_controls()

    return view_layout 