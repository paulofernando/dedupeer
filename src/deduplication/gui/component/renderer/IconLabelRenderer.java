package deduplication.gui.component.renderer;

import java.awt.Component;

import javax.swing.ImageIcon;
import javax.swing.JLabel;
import javax.swing.JTable;
import javax.swing.table.DefaultTableCellRenderer;

import deduplication.utils.FileUtils;

/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class IconLabelRenderer extends DefaultTableCellRenderer {

	private static final long serialVersionUID = 6335367333085684949L;
	
	@Override
	public Component getTableCellRendererComponent(JTable table, Object value,
			boolean isSelected, boolean hasFocus, int row, int column) {
		
		JLabel iconLabel = (JLabel) super.getTableCellRendererComponent(table, value, isSelected, hasFocus,
				row, column);
		
		ImageIcon icon = FileUtils.getIconByFileType((String) value);
		iconLabel.setIcon(icon);
		
		if(isSelected) {
			iconLabel.setBackground(table.getSelectionBackground());
			iconLabel.setForeground(table.getSelectionForeground());
		} else {
			iconLabel.setBackground(table.getBackground());
			iconLabel.setForeground(table.getForeground());
		}
		return iconLabel;
	}

}
