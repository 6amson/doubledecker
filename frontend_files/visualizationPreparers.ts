/**
 * Visualization Data Preparers
 * 
 * Prepares data specifically for each visualization type,
 * including aggregation, sampling, and metadata generation.
 */

import {
    aggregateByCategory,
    aggregateByTime,
    sampleData,
    detectColumnTypes,
    AggregatedData
} from './dataAggregation';

export interface QueryMetadata {
    hasGroupBy: boolean;
    groupByColumns: string[];
    aggregationColumns: string[];
    isAggregated: boolean;
}

export interface PreparedChartData {
    data: any[];
    metadata: {
        originalRowCount: number;
        displayedPoints: number;
        aggregationMethod: string;
        warning?: string;
        xAxisLabel?: string;
        yAxisLabel?: string;
    };
}

/**
 * Prepare data for Bar Chart
 */
export function prepareBarChartData(
    rows: Record<string, any>[],
    columns: string[],
    config?: {
        categoryColumn?: string;
        valueColumn?: string;
        topN?: number;
        queryMetadata?: QueryMetadata;
    }
): PreparedChartData {
    if (rows.length === 0 || columns.length === 0) {
        return {
            data: [],
            metadata: {
                originalRowCount: 0,
                displayedPoints: 0,
                aggregationMethod: 'No data',
            }
        };
    }

    // Smart column selection: prioritize GroupBy columns and aggregations
    const categoryColumn = config?.categoryColumn ||
        (config?.queryMetadata?.groupByColumns?.[0]) ||
        columns[0];

    const valueColumn = config?.valueColumn ||
        (config?.queryMetadata?.aggregationColumns?.[0]) ||
        undefined;

    const topN = config?.topN || 20;

    const aggregated = aggregateByCategory(
        rows,
        categoryColumn,
        valueColumn,
        valueColumn ? 'sum' : 'count',
        topN
    );

    return {
        data: aggregated,
        metadata: {
            originalRowCount: rows.length,
            displayedPoints: aggregated.length,
            aggregationMethod: `Top ${topN} by ${categoryColumn}`,
            warning: rows.length > 1000
                ? `Showing top ${topN} categories from ${rows.length.toLocaleString()} rows`
                : undefined,
            xAxisLabel: categoryColumn,
            yAxisLabel: valueColumn || 'Count'
        }
    };
}

/**
 * Prepare data for Line Chart
 */
export function prepareLineChartData(
    rows: Record<string, any>[],
    columns: string[],
    config?: {
        timeColumn?: string;
        valueColumn?: string;
        buckets?: number;
        queryMetadata?: QueryMetadata;
    }
): PreparedChartData {
    if (rows.length === 0 || columns.length === 0) {
        return {
            data: [],
            metadata: {
                originalRowCount: 0,
                displayedPoints: 0,
                aggregationMethod: 'No data',
            }
        };
    }

    const types = detectColumnTypes(rows, columns);

    // Smart column selection: prioritize GroupBy columns for time axis
    const timeColumn = config?.timeColumn ||
        (config?.queryMetadata?.groupByColumns?.[0]) ||
        Object.keys(types).find(col => types[col] === 'temporal') ||
        columns[0];

    const valueColumn = config?.valueColumn ||
        (config?.queryMetadata?.aggregationColumns?.[0]) ||
        undefined;

    const buckets = config?.buckets || Math.min(100, Math.max(10, Math.floor(rows.length / 10)));

    const aggregated = aggregateByTime(
        rows,
        timeColumn,
        valueColumn,
        valueColumn ? 'sum' : 'count',
        buckets
    );

    return {
        data: aggregated,
        metadata: {
            originalRowCount: rows.length,
            displayedPoints: aggregated.length,
            aggregationMethod: `Time series with ${buckets} buckets`,
            warning: rows.length > 10000
                ? `Aggregated ${rows.length.toLocaleString()} rows into ${buckets} time periods`
                : undefined,
            xAxisLabel: timeColumn,
            yAxisLabel: valueColumn || 'Count'
        }
    };
}

/**
 * Prepare data for Pie Chart
 */
export function preparePieChartData(
    rows: Record<string, any>[],
    columns: string[],
    config?: {
        categoryColumn?: string;
        valueColumn?: string;
        topN?: number;
        queryMetadata?: QueryMetadata;
    }
): PreparedChartData {
    if (rows.length === 0 || columns.length === 0) {
        return {
            data: [],
            metadata: {
                originalRowCount: 0,
                displayedPoints: 0,
                aggregationMethod: 'No data',
            }
        };
    }

    const topN = config?.topN || 8;
    const result = prepareBarChartData(rows, columns, { ...config, topN });

    // Add percentage
    const total = result.data.reduce((sum, item) => sum + item.value, 0);
    result.data = result.data.map(item => ({
        ...item,
        percentage: total > 0 ? (item.value / total) * 100 : 0
    }));

    result.metadata.aggregationMethod = `Top ${topN} categories`;
    result.metadata.warning = rows.length > 1000
        ? `Showing top ${topN} categories from ${rows.length.toLocaleString()} rows`
        : undefined;
    // Axis labels are inherited from prepareBarChartData

    return result;
}

/**
 * Prepare data for Scatter Plot
 */
export function prepareScatterPlotData(
    rows: Record<string, any>[],
    columns: string[],
    config?: {
        xColumn?: string;
        yColumn?: string;
        maxPoints?: number;
        queryMetadata?: QueryMetadata;
    }
): PreparedChartData {
    if (rows.length === 0 || columns.length === 0) {
        return {
            data: [],
            metadata: {
                originalRowCount: 0,
                displayedPoints: 0,
                aggregationMethod: 'No data',
            }
        };
    }

    const types = detectColumnTypes(rows, columns);
    const numericColumns = Object.keys(types).filter(col => types[col] === 'numeric');

    const xColumn = config?.xColumn || numericColumns[0] || columns[0];
    const yColumn = config?.yColumn || numericColumns[1] || columns[1] || columns[0];
    const maxPoints = config?.maxPoints || 2000;

    const sampled = sampleData(rows, maxPoints);
    const scatterData = sampled.map((row, index) => ({
        x: Number(row[xColumn]) || 0,
        y: Number(row[yColumn]) || 0,
        label: row[columns[0]] || `Point ${index + 1}` // For tooltips
    }));

    return {
        data: scatterData,
        metadata: {
            originalRowCount: rows.length,
            displayedPoints: scatterData.length,
            aggregationMethod: rows.length > maxPoints
                ? `Sampled ${maxPoints} points`
                : 'All points',
            warning: rows.length > maxPoints
                ? `Showing ${maxPoints.toLocaleString()} of ${rows.length.toLocaleString()} points`
                : undefined,
            xAxisLabel: xColumn,
            yAxisLabel: yColumn
        }
    };
}
