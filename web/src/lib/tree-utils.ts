import type { KVKey } from "./types";

export interface TreeNode {
    name: string;
    fullPath: string;
    type: 'folder' | 'key';
    children?: TreeNode[];
    keyData?: KVKey;
    count?: number;
    percentage?: number;
    level: number;
}

/**
 * Parse a key into segments based on common delimiters
 */
function parseKeySegments(key: string): string[] {
    // Split by common delimiters: : / . -
    const segments = key.split(/[:\/\.\-]/);
    return segments.filter(s => s.length > 0);
}

/**
 * Build a hierarchical tree structure from a flat list of keys
 */
export function buildKeyTree(keys: KVKey[]): TreeNode[] {
    const root: Map<string, TreeNode> = new Map();
    const totalKeys = keys.length;

    // Build tree structure
    keys.forEach(keyData => {
        const segments = parseKeySegments(keyData.key);
        let currentLevel = root;
        let currentPath = '';

        segments.forEach((segment, index) => {
            const isLastSegment = index === segments.length - 1;
            currentPath = currentPath ? `${currentPath}:${segment}` : segment;

            if (!currentLevel.has(segment)) {
                const node: TreeNode = {
                    name: segment,
                    fullPath: currentPath,
                    type: isLastSegment ? 'key' : 'folder',
                    level: index,
                    children: isLastSegment ? undefined : [],
                    keyData: isLastSegment ? keyData : undefined,
                };
                currentLevel.set(segment, node);
            }

            const node = currentLevel.get(segment)!;

            // If this is not the last segment, traverse deeper
            if (!isLastSegment && node.children) {
                // Create children map if it doesn't exist
                if (!node.children.length) {
                    currentLevel = new Map();
                } else {
                    currentLevel = new Map(node.children.map(child => [child.name, child]));
                }
            }
        });
    });

    // Convert map to array and calculate statistics
    const treeArray = Array.from(root.values());
    calculateTreeStats(treeArray, totalKeys);

    return treeArray;
}

/**
 * Calculate statistics (count, percentage) for tree nodes
 */
function calculateTreeStats(nodes: TreeNode[], totalKeys: number): void {
    nodes.forEach(node => {
        if (node.type === 'folder' && node.children) {
            // Recursively calculate for children first
            calculateTreeStats(node.children, totalKeys);

            // Count total keys in this folder
            node.count = countKeysInNode(node);
            node.percentage = Math.round((node.count / totalKeys) * 100);
        } else if (node.type === 'key') {
            node.count = 1;
        }
    });
}

/**
 * Count total keys in a node (including nested children)
 */
function countKeysInNode(node: TreeNode): number {
    if (node.type === 'key') {
        return 1;
    }

    if (node.type === 'folder' && node.children) {
        return node.children.reduce((sum, child) => sum + countKeysInNode(child), 0);
    }

    return 0;
}

/**
 * Flatten tree to get all keys (for search filtering)
 */
export function flattenTree(nodes: TreeNode[]): KVKey[] {
    const keys: KVKey[] = [];

    function traverse(node: TreeNode) {
        if (node.type === 'key' && node.keyData) {
            keys.push(node.keyData);
        }
        if (node.children) {
            node.children.forEach(traverse);
        }
    }

    nodes.forEach(traverse);
    return keys;
}

/**
 * Filter tree by search query
 */
export function filterTree(nodes: TreeNode[], query: string): TreeNode[] {
    if (!query) return nodes;

    const lowerQuery = query.toLowerCase();

    function filterNode(node: TreeNode): TreeNode | null {
        // If it's a key, check if it matches
        if (node.type === 'key') {
            return node.name.toLowerCase().includes(lowerQuery) ? node : null;
        }

        // If it's a folder, filter children
        if (node.children) {
            const filteredChildren = node.children
                .map(filterNode)
                .filter((child): child is TreeNode => child !== null);

            // If folder has matching children or name matches, include it
            if (filteredChildren.length > 0 || node.name.toLowerCase().includes(lowerQuery)) {
                return {
                    ...node,
                    children: filteredChildren,
                    count: filteredChildren.reduce((sum, child) => sum + (child.count || 0), 0),
                };
            }
        }

        return null;
    }

    return nodes
        .map(filterNode)
        .filter((node): node is TreeNode => node !== null);
}
