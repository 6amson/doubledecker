/**
 * Data Aggregation Utilities
 * 
 * Provides functions for aggregating, sampling, and analyzing data
 * to prepare it for various visualization types.
 */

export interface AggregatedData {
    label: string;
    value: number;
    count?: number;
    percentage?: number;
}

export type ColumnType = 'numeric' | 'temporal' | 'categorical';

/**
 * Aggregate categorical data - for Bar/Pie charts
 * Groups by a column and counts/sums values
 */
export function aggregateByCategory(
    rows: Record<string, any>[],
    categoryColumn: string,
    valueColumn?: string,
    aggregateFunc: 'count' | 'sum' | 'avg' = 'count',
    topN: number = 20
): AggregatedData[] {
    const grouped = new Map<string, number[]>();

    rows.forEach(row => {
        const category = String(row[categoryColumn] ?? 'Unknown');
        const value = valueColumn ? (Number(row[valueColumn]) || 0) : 1;

        if (!grouped.has(category)) {
            grouped.set(category, []);
        }
        grouped.get(category)!.push(value);
    });

    const aggregated = Array.from(grouped.entries()).map(([label, values]) => {
        let value: number;
        switch (aggregateFunc) {
            case 'sum':
                value = values.reduce((a, b) => a + b, 0);
                break;
            case 'avg':
                value = values.reduce((a, b) => a + b, 0) / values.length;
                break;
            case 'count':
            default:
                value = values.length;
                break;
        }

        return { label, value, count: values.length };
    });

    // Sort by value descending
    aggregated.sort((a, b) => b.value - a.value);

    // Take top N
    const topItems = aggregated.slice(0, topN);

    // If there are more items, create "Others" category
    if (aggregated.length > topN) {
        const othersValue = aggregated
            .slice(topN)
            .reduce((sum, item) => sum + item.value, 0);
        const othersCount = aggregated
            .slice(topN)
            .reduce((sum, item) => sum + (item.count || 0), 0);

        topItems.push({
            label: `Others (${aggregated.length - topN})`,
            value: othersValue,
            count: othersCount
        });
    }

    return topItems;
}

/**
 * Time-based aggregation - for Line/Area charts
 */
export function aggregateByTime(
    rows: Record<string, any>[],
    timeColumn: string,
    valueColumn?: string,
    aggregateFunc: 'count' | 'sum' | 'avg' = 'count',
    buckets: number = 100
): AggregatedData[] {
    // Parse dates and find range
    const dataPoints = rows
        .map(row => ({
            time: new Date(row[timeColumn]).getTime(),
            value: valueColumn ? (Number(row[valueColumn]) || 0) : 1
        }))
        .filter(d => !isNaN(d.time));

    if (dataPoints.length === 0) return [];

    const minTime = Math.min(...dataPoints.map(d => d.time));
    const maxTime = Math.max(...dataPoints.map(d => d.time));
    const bucketSize = (maxTime - minTime) / buckets;

    if (bucketSize === 0) {
        // All dates are the same
        const totalValue = dataPoints.reduce((sum, d) => sum + d.value, 0);
        return [{
            label: new Date(minTime).toISOString().split('T')[0],
            value: aggregateFunc === 'avg' ? totalValue / dataPoints.length : totalValue,
            count: dataPoints.length
        }];
    }

    const bucketMap = new Map<number, number[]>();

    dataPoints.forEach(({ time, value }) => {
        const bucketIndex = Math.floor((time - minTime) / bucketSize);
        if (!bucketMap.has(bucketIndex)) {
            bucketMap.set(bucketIndex, []);
        }
        bucketMap.get(bucketIndex)!.push(value);
    });

    return Array.from(bucketMap.entries())
        .map(([bucketIndex, values]) => {
            const bucketTime = minTime + (bucketIndex * bucketSize);
            let value: number;

            switch (aggregateFunc) {
                case 'sum':
                    value = values.reduce((a, b) => a + b, 0);
                    break;
                case 'avg':
                    value = values.reduce((a, b) => a + b, 0) / values.length;
                    break;
                case 'count':
                default:
                    value = values.length;
                    break;
            }

            return {
                label: new Date(bucketTime).toISOString().split('T')[0],
                value,
                count: values.length
            };
        })
        .sort((a, b) => new Date(a.label).getTime() - new Date(b.label).getTime());
}

/**
 * Random sampling - for Scatter plots
 */
export function sampleData<T>(
    data: T[],
    maxSamples: number = 1000,
    method: 'random' | 'systematic' = 'systematic'
): T[] {
    if (data.length <= maxSamples) return data;

    if (method === 'random') {
        // Random sampling
        const sampled: T[] = [];
        const indices = new Set<number>();

        while (indices.size < maxSamples) {
            indices.add(Math.floor(Math.random() * data.length));
        }

        Array.from(indices).sort((a, b) => a - b).forEach(i => {
            sampled.push(data[i]);
        });

        return sampled;
    } else {
        // Systematic sampling (every nth item)
        const step = Math.floor(data.length / maxSamples);
        return data.filter((_, index) => index % step === 0).slice(0, maxSamples);
    }
}

/**
 * Parse numeric value with support for various formats
 */
function parseNumericValue(value: any): number | null {
    if (value === null || value === undefined || value === '') return null;

    // Convert to string for processing
    let str = String(value).trim();

    // Remove currency symbols
    str = str.replace(/[$€£¥]/g, '');

    // Handle percentages
    const isPercentage = str.endsWith('%');
    if (isPercentage) {
        str = str.slice(0, -1);
    }

    // Remove commas (for large numbers like "1,234,567")
    str = str.replace(/,/g, '');

    // Parse the number
    const num = Number(str);

    if (isNaN(num)) return null;

    // If it was a percentage, divide by 100
    return isPercentage ? num / 100 : num;
}

/**
 * Check if column name suggests it's an ID field
 */
function isIdColumn(columnName: string): boolean {
    const lowerName = columnName.toLowerCase();
    return lowerName.includes('id') ||
        lowerName.includes('_id') ||
        lowerName === 'id' ||
        lowerName.endsWith('_key');
}

/**
 * Detect column types based on sample data
 */
export function detectColumnTypes(
    rows: Record<string, any>[],
    columns: string[]
): Record<string, ColumnType> {
    const types: Record<string, ColumnType> = {};

    columns.forEach(col => {
        const sample = rows.slice(0, Math.min(100, rows.length)).map(row => row[col]);

        // Check if numeric (with enhanced parsing)
        const parsedNumbers = sample.map(v => parseNumericValue(v));
        const numericCount = parsedNumbers.filter(n => n !== null).length;

        if (numericCount > sample.length * 0.8) {
            // It's mostly numeric, but check for categorical edge cases

            // Check if it's an ID column
            if (isIdColumn(col)) {
                types[col] = 'categorical';
                return;
            }

            // Check for low cardinality (likely categorical like ratings, status codes)
            const validNumbers = parsedNumbers.filter(n => n !== null) as number[];
            const uniqueValues = new Set(validNumbers);

            if (uniqueValues.size <= 10 && uniqueValues.size < validNumbers.length * 0.5) {
                // Low cardinality with many repeats = categorical
                types[col] = 'categorical';
                return;
            }

            types[col] = 'numeric';
            return;
        }

        // Check if temporal
        const dateCount = sample.filter(v => {
            if (!v) return false;

            // Reject pure numbers (avoid false positives)
            if (typeof v === 'number' || !isNaN(Number(v))) {
                // Only accept if it looks like a Unix timestamp (10+ digits)
                const numStr = String(v);
                if (numStr.length < 10) return false;
            }

            const date = new Date(v);
            const isValid = !isNaN(date.getTime());

            // Additional validation: reject dates that are too far in past/future
            if (isValid) {
                const year = date.getFullYear();
                return year >= 1900 && year <= 2100;
            }

            return false;
        }).length;

        if (dateCount > sample.length * 0.8) {
            types[col] = 'temporal';
            return;
        }

        // Default to categorical
        types[col] = 'categorical';
    });

    return types;
}

/**
 * Suggest best visualization based on data
 */
export function suggestVisualization(
    rows: Record<string, any>[],
    columns: string[]
): string[] {
    if (rows.length === 0 || columns.length === 0) {
        return ['table'];
    }

    const types = detectColumnTypes(rows, columns);
    const suggestions: string[] = ['table']; // Always available

    const hasNumeric = Object.values(types).includes('numeric');
    const hasTemporal = Object.values(types).includes('temporal');
    const hasCategorical = Object.values(types).includes('categorical');

    if (hasCategorical) {
        suggestions.push('bar', 'pie');
    }

    if (hasTemporal) {
        suggestions.push('line');
    }

    if (hasNumeric && columns.length >= 2) {
        const numericColumns = Object.keys(types).filter(col => types[col] === 'numeric');
        if (numericColumns.length >= 2) {
            suggestions.push('scatter');
        }
    }

    return suggestions;
}
