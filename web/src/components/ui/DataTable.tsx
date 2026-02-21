import { ArrowDown, ArrowUp, ArrowUpDown } from 'lucide-react';
import { cn } from '../../lib/utils';

// =============================================================================
// Generic DataTable component
// Usage:
//   <DataTable
//     columns={[{ key: 'name', label: 'Name', width: '30%', sortable: true }, ...]}
//     rows={data}
//     sortKey="name"
//     sortDir="asc"
//     onSort={(key) => handleSort(key)}
//     onRowClick={(row) => navigate(row.id)}
//     emptyState={<p>No items found.</p>}
//   />
// =============================================================================

export interface TableColumn<T> {
  /** Unique key, also used to pull value from row if renderCell not provided */
  key: keyof T | string;
  label: string;
  width?: string;
  align?: 'left' | 'right' | 'center';
  renderCell?: (row: T) => React.ReactNode;
  /** If true, clicking the column header fires onSort */
  sortable?: boolean;
}

interface DataTableProps<T> {
  columns: TableColumn<T>[];
  rows: T[];
  /** Called when a row is clicked. Enables hover state on rows. */
  onRowClick?: (row: T) => void;
  /** Rendered when rows is empty */
  emptyState?: React.ReactNode;
  /** Extra className on the wrapping div */
  className?: string;
  /** Key extractor for React reconciliation */
  rowKey: (row: T) => string;
  /** Currently active sort column key */
  sortKey?: string;
  /** Current sort direction */
  sortDir?: 'asc' | 'desc';
  /** Called with the column key when a sortable header is clicked */
  onSort?: (key: string) => void;
}

export function DataTable<T>({
  columns,
  rows,
  onRowClick,
  emptyState,
  className,
  rowKey,
  sortKey,
  sortDir,
  onSort,
}: DataTableProps<T>) {
  return (
    <div className={cn('w-full rounded-lg border border-surface-border overflow-hidden', className)}>
      <div className="overflow-auto">
      <table className="w-full text-sm text-left">
        <thead>
          <tr className="border-b-2 border-surface-border bg-surface">
            {columns.map((col) => {
              const isActive = sortKey === String(col.key);
              const isSortable = col.sortable && onSort;
              return (
              <th
                key={String(col.key)}
                style={{ width: col.width }}
                onClick={isSortable ? () => onSort!(String(col.key)) : undefined}
                className={cn(
                  'h-10 px-4 align-middle font-medium text-text-secondary text-xs uppercase tracking-wider select-none group/th',
                  col.align === 'right' && 'text-right',
                  col.align === 'center' && 'text-center',
                  isSortable && 'cursor-pointer hover:text-text-primary transition-colors'
                )}
              >
                {isSortable ? (
                  <span className="inline-flex items-center gap-1.5">
                    {col.label}
                    <span className={cn(
                      'transition-opacity',
                      isActive ? 'opacity-100 text-text-primary' : 'opacity-0 group-hover/th:opacity-60'
                    )}>
                      {isActive
                        ? (sortDir === 'desc' ? <ArrowDown className="w-3 h-3" /> : <ArrowUp className="w-3 h-3" />)
                        : <ArrowUpDown className="w-3 h-3" />}
                    </span>
                  </span>
                ) : col.label}
              </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {rows.length === 0 ? (
            <tr>
              <td colSpan={columns.length} className="py-12 px-4 text-center text-text-secondary">
                {emptyState ?? <span className="text-sm">No results.</span>}
              </td>
            </tr>
          ) : (
            rows.map((row) => (
              <tr
                key={rowKey(row)}
                onClick={onRowClick ? () => onRowClick(row) : undefined}
                className={cn(
                  'border-b border-surface-border last:border-0 transition-colors',
                  onRowClick && 'cursor-pointer hover:bg-surface-hover group'
                )}
              >
                {columns.map((col) => (
                  <td
                    key={String(col.key)}
                    className={cn(
                      'px-4 py-3 align-middle',
                      col.align === 'right' && 'text-right',
                      col.align === 'center' && 'text-center'
                    )}
                  >
                    {col.renderCell
                      ? col.renderCell(row)
                      : String((row as Record<string, unknown>)[String(col.key)] ?? '-')}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
      </div>
    </div>
  );
}
