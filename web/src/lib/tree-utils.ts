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
 * Parse a key into path segments using '/' and ':' as delimiters.
 * Covers filesystem-style paths (checkpoints/abc) and Redis-style
 * namespacing (user:123:profile). Other characters (-, .) are
 * preserved so UUIDs and dotted names stay intact.
 */
function parseKeySegments(key: string): string[] {
    return key.split(/[/:]/).filter(s => s.length > 0);
}

/**
 * Build a hierarchical tree structure from a flat list of keys.
 * Uses a trie internally so that children are properly connected.
 */
export function buildKeyTree(keys: KVKey[]): TreeNode[] {
    interface TrieNode {
        children: Map<string, TrieNode>;
        keyData?: KVKey;
    }

    const root: TrieNode = { children: new Map() };
    const totalKeys = keys.length;

    // Insert all keys into a trie
    keys.forEach(keyData => {
        const segments = parseKeySegments(keyData.key);
        let current = root;
        segments.forEach((segment, i) => {
            if (!current.children.has(segment)) {
                current.children.set(segment, { children: new Map() });
            }
            current = current.children.get(segment)!;
            if (i === segments.length - 1) {
                current.keyData = keyData;
            }
        });
    });

    // Convert trie → TreeNode[]
    function materialize(trieChildren: Map<string, TrieNode>, level: number, pathPrefix: string): TreeNode[] {
        return Array.from(trieChildren.entries()).map(([name, trie]) => {
            const fullPath = pathPrefix ? `${pathPrefix}/${name}` : name;
            const hasChildren = trie.children.size > 0;
            return {
                name,
                fullPath: trie.keyData ? trie.keyData.key : fullPath,
                type: (hasChildren ? 'folder' : 'key') as 'folder' | 'key',
                level,
                children: hasChildren ? materialize(trie.children, level + 1, fullPath) : undefined,
                keyData: trie.keyData,
            };
        });
    }

    const treeArray = materialize(root.children, 0, '');
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
