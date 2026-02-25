
TODO

- null orange marker in column list
- navigation keys for column list, space, ctrl-f ctrl-b paging, top of list, top of page, bottom of page
- data pane cross hair movement, when right column is selected moving left scrolls the data keeping cursor in right column instead of moving cursor, data shouldn't scroll left until cursor selected column is at left most visible column (same as vertical movement)
- esc to clear search string
  
FILE OPEN
- ctrl-o fuzzy search open file parquet or csv matching

UI/UX
- ctrl-l to force redraw
- data pane doesnt resize height (max 50 rows)
- add the orange dot column nan to column pane
- when i scroll the column pane with the mouse the data pane scrolls up so the header row and first rows are not visible
- --help arg
- show histogram above columns?
- for details default to stats pain for numerical columns, and top for catergorical (based on cardinality)
- cell navigation:
  - gg: top left cell
  - navigate tor row/column, vim style? how to handle high row numbers
- resizing:
  - resizing panes
  - resizing column width
- how to handle longer column names?
- data filtering:
  - filter on this value (e.g. in a user id)
  - possibly add this user-id to filter set? (more complicated interface)
- column pane navigation
  - jump to top, 
  - fuzzy search
  - f, go to next 

NULL FEATURE
- r, jump to column with next null in this row
- c, jump to row next null in this column


BUGS
- column pane too long lose title bar without a redraw
